from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from alt_hot_scanner.data.binance_public import (
    FUTURES_SERVER_TIME_URL,
    parse_futures_server_time,
    validate_archive_object_key,
)
from alt_hot_scanner.universe.adjudications import (
    load_lifecycle_adjudications,
    require_exclusive_utc_day_boundary,
    require_exclusive_utc_timestamp,
)
from alt_hot_scanner.universe.contracts import filter_instrument_scope
from alt_hot_scanner.universe.delisting_registry import cms_corpus_binding
from alt_hot_scanner.universe.eligibility_oracle import run_eligibility_oracle
from alt_hot_scanner.universe.lifecycle import CATALOG_SCHEMA_VERSION
from alt_hot_scanner.universe.scope_registry import (
    candidate_set_digest,
    verify_scope_registry,
)
from alt_hot_scanner.utils.config import load_config

BUNDLE_SCHEMA_VERSION = "lifecycle-authorization-bundle-v5"
FRESHNESS_SCHEMA_VERSION = "lifecycle-freshness-v1"
APPROVAL_SCHEMA_VERSION = "lifecycle-approval-pin-v2"
APPROVAL_STATE_SCHEMA_VERSION = "lifecycle-approval-state-registry-v1"
PLAN_SCHEMA_VERSION = "full-history-download-plan-v4"
PLAN_FIELDS = {
    "schema_version",
    "created_at",
    "purpose",
    "source",
    "lifecycle_bundle",
    "lifecycle_bundle_id",
    "lifecycle_approval",
    "lifecycle_approval_id",
    "approval_state_registry",
    "artifact_hashes",
    "config_sha256",
    "readiness",
    "planning_basis",
    "warmup_start_month",
    "end_month",
    "symbols_discovered",
    "objects",
    "plan_integrity",
}


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def sha256_path(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def content_identity(value: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _parse_utc(value: str, field: str) -> datetime:
    require_exclusive_utc_timestamp(value, field)
    return datetime.fromisoformat(value)


def build_lifecycle_freshness(
    *,
    required_valid_through_utc: str,
    candidate_valid_through_utc: str,
    episode_valid_through_utc: str,
    delisting_valid_through_utc: str,
    server_time_evidence: dict[str, Any],
    frontier_candidate_discovery: dict[str, Any],
    episode_freshness_evidence: dict[str, Any],
    announcement_corpus: dict[str, Any],
    bound_raw_evidence: list[dict[str, Any]],
) -> dict[str, Any]:
    require_exclusive_utc_day_boundary(
        required_valid_through_utc, "required_valid_through_utc"
    )
    for name, value in (
        ("candidate_valid_through_utc", candidate_valid_through_utc),
        ("episode_valid_through_utc", episode_valid_through_utc),
        ("delisting_valid_through_utc", delisting_valid_through_utc),
    ):
        require_exclusive_utc_timestamp(value, name)
    common = min(
        candidate_valid_through_utc,
        episode_valid_through_utc,
        delisting_valid_through_utc,
        key=lambda value: _parse_utc(value, "component horizon"),
    )
    required_dt = _parse_utc(required_valid_through_utc, "required_valid_through_utc")
    if _parse_utc(common, "lifecycle_evidence_valid_through_utc") < required_dt:
        raise ValueError("Lifecycle evidence cannot support the required freshness horizon")
    core = {
        "schema_version": FRESHNESS_SCHEMA_VERSION,
        "boundary_semantics": "exclusive",
        "required_valid_through_utc": required_valid_through_utc,
        "candidate_valid_through_utc": candidate_valid_through_utc,
        "episode_valid_through_utc": episode_valid_through_utc,
        "delisting_valid_through_utc": delisting_valid_through_utc,
        "lifecycle_evidence_valid_through_utc": common,
        "binance_server_time_evidence": server_time_evidence,
        "frontier_candidate_discovery": frontier_candidate_discovery,
        "episode_freshness_evidence": episode_freshness_evidence,
        "announcement_corpus": announcement_corpus,
        "bound_raw_evidence": bound_raw_evidence,
    }
    return {**core, "freshness_id": content_identity(core)}


def validate_lifecycle_freshness(
    payload: dict[str, Any], *, report_root: str | Path | None = None
) -> dict[str, Any]:
    """Validate exact freshness structure, identities, horizons, and bound raw bytes."""
    required = {
        "schema_version",
        "boundary_semantics",
        "required_valid_through_utc",
        "candidate_valid_through_utc",
        "episode_valid_through_utc",
        "delisting_valid_through_utc",
        "lifecycle_evidence_valid_through_utc",
        "binance_server_time_evidence",
        "frontier_candidate_discovery",
        "episode_freshness_evidence",
        "announcement_corpus",
        "bound_raw_evidence",
        "freshness_id",
    }
    if type(payload) is not dict or set(payload) != required:
        raise ValueError("Lifecycle freshness artifact has the wrong schema")
    if payload["schema_version"] != FRESHNESS_SCHEMA_VERSION:
        raise ValueError("Lifecycle freshness artifact has an unsupported schema")
    if payload["boundary_semantics"] != "exclusive":
        raise ValueError("Lifecycle freshness artifact must use exclusive boundaries")
    require_exclusive_utc_day_boundary(
        payload["required_valid_through_utc"], "required_valid_through_utc"
    )
    for field in (
        "candidate_valid_through_utc",
        "episode_valid_through_utc",
        "delisting_valid_through_utc",
        "lifecycle_evidence_valid_through_utc",
    ):
        require_exclusive_utc_timestamp(payload[field], field)
    expected_common = min(
        (payload[field] for field in (
            "candidate_valid_through_utc",
            "episode_valid_through_utc",
            "delisting_valid_through_utc",
        )),
        key=lambda value: _parse_utc(value, "component horizon"),
    )
    if payload["lifecycle_evidence_valid_through_utc"] != expected_common:
        raise ValueError("Lifecycle freshness common horizon is not the component minimum")
    if _parse_utc(expected_common, "common horizon") < _parse_utc(
        payload["required_valid_through_utc"], "required_valid_through_utc"
    ):
        raise ValueError("Lifecycle freshness is below the required horizon")
    core = {key: value for key, value in payload.items() if key != "freshness_id"}
    if payload["freshness_id"] != content_identity(core):
        raise ValueError("Lifecycle freshness identity is invalid")

    descriptors = payload["bound_raw_evidence"]
    if type(descriptors) is not list or len({item.get("path") for item in descriptors}) != len(descriptors):
        raise ValueError("Lifecycle freshness raw evidence descriptors are not unique")
    for descriptor in descriptors:
        if type(descriptor) is not dict or set(descriptor) != {"path", "sha256"}:
            raise ValueError("Lifecycle freshness raw evidence descriptor is malformed")
        if not isinstance(descriptor["path"], str) or not isinstance(
            descriptor["sha256"], str
        ) or len(descriptor["sha256"]) != 64 or any(
            character not in "0123456789abcdef" for character in descriptor["sha256"]
        ):
            raise ValueError("Lifecycle freshness raw evidence descriptor is invalid")
        if report_root is not None and sha256_path(descriptor["path"]) != descriptor["sha256"]:
            raise ValueError(f"Lifecycle freshness raw evidence hash mismatch: {descriptor['path']}")
    server = payload["binance_server_time_evidence"]
    if type(server) is not dict or not {"path", "sha256", "source_url", "server_time_utc", "server_date_utc", "maximum_archive_backed_horizon_utc"}.issubset(server):
        raise ValueError("Binance server-time evidence descriptor is incomplete")
    require_exclusive_utc_timestamp(server["server_time_utc"], "server_time_utc")
    if server["source_url"] != FUTURES_SERVER_TIME_URL:
        raise ValueError("Binance server-time evidence has the wrong source URL")
    require_exclusive_utc_day_boundary(
        server["maximum_archive_backed_horizon_utc"],
        "maximum_archive_backed_horizon_utc",
    )
    if report_root is not None and sha256_path(server["path"]) != server["sha256"]:
        raise ValueError("Binance server-time evidence hash mismatch")
    if report_root is not None:
        observed_server_time = parse_futures_server_time(Path(server["path"]).read_bytes())
        expected_server_time = observed_server_time.isoformat().replace("+00:00", "Z")
        expected_server_date = observed_server_time.date().isoformat()
        expected_archive_horizon = (
            observed_server_time.replace(hour=0, minute=0, second=0, microsecond=0)
            - timedelta(days=1)
        ).isoformat().replace("+00:00", "Z")
        if (
            server["server_time_utc"] != expected_server_time
            or server["server_date_utc"] != expected_server_date
            or server["maximum_archive_backed_horizon_utc"] != expected_archive_horizon
        ):
            raise ValueError("Binance server-time descriptor disagrees with its raw evidence")
    if {item["path"] for item in descriptors} != {
        item["path"] for item in descriptors
    } | {server["path"]}:
        raise ValueError("Freshness raw evidence does not include server-time evidence")
    corpus = payload["announcement_corpus"]
    if type(corpus) is not dict or set(corpus) != {
        "identity", "sha256", "acquisition_started_at_utc"
    }:
        raise ValueError("Announcement corpus freshness binding is malformed")
    if corpus["identity"] != corpus["sha256"] or len(corpus["identity"]) != 64:
        raise ValueError("Announcement corpus freshness binding is invalid")
    require_exclusive_utc_timestamp(
        corpus["acquisition_started_at_utc"], "acquisition_started_at_utc"
    )
    if _parse_utc(
        payload["delisting_valid_through_utc"], "delisting_valid_through_utc"
    ) > _parse_utc(corpus["acquisition_started_at_utc"], "acquisition_started_at_utc"):
        raise ValueError("Delisting freshness exceeds the frozen CMS acquisition start")
    return payload


def catalog_readiness(catalog: pd.DataFrame, noncanonical_candidates: list[str]) -> dict[str, Any]:
    in_scope = (
        catalog["scope_disposition"].isin(["in_scope_crypto_perpetual", "benchmark_only"])
        if "scope_disposition" in catalog.columns
        else catalog["symbol"].str.endswith("USDT")
    )
    derived_ready = (
        in_scope
        & catalog["scope_classification_complete"].eq(True)
        & catalog["eligibility_age_anchor_at"].notna()
        & catalog["eligibility_age_anchor_basis"].ne("unresolved")
        & catalog["delisting_evidence_state"].isin(
            [
                "not_applicable_currently_trading",
                "exact_applicable_announcement_publication",
                "official_search_completed_no_reliable_announcement_timestamp",
                "resolved_multi_episode_currently_trading",
                "resolved_multi_episode_terminated",
                "reviewed_terminal_episode",
            ]
        )
    )
    supplied_ready = catalog["historical_inclusion_readiness"].eq("ready")
    if not supplied_ready.equals(derived_ready):
        raise ValueError("Derived catalog readiness disagrees with primitive lifecycle fields")
    row_blockers = sorted(
        catalog.loc[
            in_scope & ~derived_ready,
            "symbol",
        ].tolist()
    )
    external = sorted(noncanonical_candidates)
    return {
        "schema_version": "lifecycle-readiness-v1",
        "catalog_schema_version": CATALOG_SCHEMA_VERSION,
        "candidate_rows": int(in_scope.sum()),
        "ready_rows": int(derived_ready.sum()),
        "catalog_blocker_count": len(row_blockers),
        "catalog_blocker_symbols": row_blockers,
        "noncanonical_blocker_count": len(external),
        "noncanonical_blockers": external,
        "authorization_ready": not row_blockers and not external,
    }


def build_bundle_payload(
    *,
    report_root: Path,
    config_path: Path,
    artifact_names: list[str],
    code_commit: str,
    created_at: str,
) -> dict[str, Any]:
    if "lifecycle_freshness.json" not in artifact_names:
        raise ValueError("Lifecycle bundle must bind lifecycle_freshness.json")
    artifacts = {
        name: {"path": name, "sha256": sha256_path(report_root / name)}
        for name in sorted(artifact_names)
    }
    scope = json.loads((report_root / "historical_scope_registry.json").read_text("utf-8"))
    primitive = json.loads(
        (report_root / "primitive_evidence_manifest.json").read_text("utf-8")
    )
    replay = json.loads(
        (report_root / "independent_eligibility_verification_report.json").read_text("utf-8")
    )
    delisting = json.loads(
        (report_root / "historical_delisting_cutoff_registry.json").read_text("utf-8")
    )
    delisting_review = json.loads(
        (report_root / "delisting_registry_independent_review.json").read_text("utf-8")
    )
    episodes = json.loads(
        (report_root / "episode_first_observed_trades.json").read_text("utf-8")
    )
    adjudications = json.loads(
        (report_root / "lifecycle_adjudications.json").read_text("utf-8")
    )
    freshness = json.loads(
        (report_root / "lifecycle_freshness.json").read_text("utf-8")
    )
    validate_lifecycle_freshness(freshness, report_root=report_root)
    core = {
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "purpose": "content_bound_lifecycle_authorization",
        "created_at": created_at,
        "catalog_schema_version": CATALOG_SCHEMA_VERSION,
        "lifecycle_evidence_valid_through_utc": freshness[
            "lifecycle_evidence_valid_through_utc"
        ],
        "artifacts": artifacts,
        "config": {
            "path": str(config_path.resolve()),
            "sha256": sha256_path(config_path),
        },
        "code_commit": code_commit,
        "authorization_chain": {
            "primitive_evidence_manifest_id": primitive["manifest_id"],
            "primitive_evidence_manifest_sha256": artifacts[
                "primitive_evidence_manifest.json"
            ]["sha256"],
            "eligibility_oracle_report_id": replay["verification_report_id"],
            "eligibility_oracle_report_sha256": artifacts[
                "independent_eligibility_verification_report.json"
            ]["sha256"],
            "reviewed_scope_registry_id": scope["registry_id"],
            "reviewed_scope_registry_sha256": artifacts[
                "historical_scope_registry.json"
            ]["sha256"],
            "scope_independent_review": scope["independent_review"],
            "lifecycle_adjudication_id": adjudications["adjudication_id"],
            "reviewed_delisting_registry_id": delisting["registry_id"],
            "reviewed_delisting_registry_sha256": artifacts[
                "historical_delisting_cutoff_registry.json"
            ]["sha256"],
            "delisting_independent_review_id": delisting_review["review_id"],
            "delisting_independent_review_sha256": artifacts[
                "delisting_registry_independent_review.json"
            ]["sha256"],
            "episode_boundary_evidence_id": episodes["evidence_id"],
            "episode_boundary_evidence_sha256": artifacts[
                "episode_first_observed_trades.json"
            ]["sha256"],
            "lifecycle_freshness_id": freshness["freshness_id"],
            "lifecycle_freshness_sha256": artifacts["lifecycle_freshness.json"]["sha256"],
        },
    }
    return {**core, "bundle_id": content_identity(core)}


def verify_approval_pin(
    approval_path: str | Path, verified_bundle: dict[str, Any]
) -> dict[str, Any]:
    path = Path(approval_path).resolve(strict=True)
    approval = json.loads(path.read_text(encoding="utf-8"))
    required = {
        "schema_version",
        "lifecycle_bundle_id",
        "lifecycle_evidence_code_commit",
        "config_sha256",
        "independent_review_artifact",
        "approval_purpose",
        "approval_status",
        "approval_state_registry",
        "approved_at",
        "approval_id",
    }
    if type(approval) is not dict or set(approval) != required:
        raise ValueError("Lifecycle approval fields do not match the exact schema")
    core = {key: value for key, value in approval.items() if key != "approval_id"}
    if approval.get("schema_version") != APPROVAL_SCHEMA_VERSION:
        raise ValueError("Lifecycle approval has an unsupported schema")
    if approval.get("approval_id") != content_identity(core):
        raise ValueError("Lifecycle approval identity is invalid")
    bundle = verified_bundle["bundle"]
    if approval.get("lifecycle_bundle_id") != bundle["bundle_id"]:
        raise ValueError("Lifecycle approval pins the wrong bundle ID")
    if approval.get("lifecycle_evidence_code_commit") != bundle["code_commit"]:
        raise ValueError("Lifecycle approval pins the wrong code commit")
    if approval.get("config_sha256") != bundle["config"]["sha256"]:
        raise ValueError("Lifecycle approval pins the wrong config")
    if approval.get("approval_purpose") != "full_history_acquisition_after_lifecycle_audit":
        raise ValueError("Lifecycle approval has the wrong purpose")
    if approval.get("approval_status") != "active":
        raise ValueError("Lifecycle approval pin is not active")
    review = approval.get("independent_review_artifact")
    if type(review) is not dict or set(review) != {"identifier", "path", "sha256"}:
        raise ValueError("Lifecycle approval review artifact is malformed")
    review_path = Path(review["path"])
    if not review_path.is_absolute() or sha256_path(review_path) != review["sha256"]:
        raise ValueError("Lifecycle approval review artifact digest is invalid")
    state_descriptor = approval.get("approval_state_registry")
    if type(state_descriptor) is not dict or set(state_descriptor) != {"path"}:
        raise ValueError("Lifecycle approval state-registry descriptor is malformed")
    state_path = Path(state_descriptor["path"])
    if not state_path.is_absolute():
        raise ValueError("Lifecycle approval state registry path must be absolute")
    state_payload = state_path.read_bytes()
    state_registry = json.loads(state_payload)
    if state_registry.get("schema_version") != APPROVAL_STATE_SCHEMA_VERSION:
        raise ValueError("Lifecycle approval state registry has an unsupported schema")
    state_core = {key: value for key, value in state_registry.items() if key != "registry_id"}
    if state_registry.get("registry_id") != content_identity(state_core):
        raise ValueError("Lifecycle approval state registry identity is invalid")
    states = state_registry.get("approvals")
    state = states.get(approval["approval_id"]) if type(states) is dict else None
    if type(state) is not dict or set(state) != {"status", "superseded_by", "reason"}:
        raise ValueError("Lifecycle approval has no exact state-registry entry")
    if state.get("status") not in {"active", "revoked", "superseded"}:
        raise ValueError("Lifecycle approval state is unsupported")
    if state.get("status") != "active":
        raise ValueError(f"Lifecycle approval is {state.get('status')}")
    return {
        "approval": approval,
        "approval_path": path,
        "state_registry": state_registry,
        "state_registry_path": state_path,
        "state_registry_sha256": hashlib.sha256(state_payload).hexdigest(),
    }


def verify_runtime_matches_approved_commit(root: str | Path, code_commit: str) -> None:
    """Permit later review-doc commits but reject changes to executable lifecycle code/config."""
    result = subprocess.run(
        [
            "git",
            "diff",
            "--quiet",
            code_commit,
            "--",
            "src",
            "scripts",
            "config",
            "pyproject.toml",
        ],
        cwd=Path(root),
        check=False,
    )
    if result.returncode != 0:
        raise ValueError("Runtime code/config differs from the approved lifecycle commit")
    untracked = subprocess.run(
        [
            "git",
            "status",
            "--porcelain",
            "--untracked-files=all",
            "--",
            "src",
            "scripts",
            "config",
            "pyproject.toml",
        ],
        cwd=Path(root),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if any(line.startswith("?? ") for line in untracked.splitlines()):
        raise ValueError("Runtime code/config contains unapproved untracked files")


def _verify_catalog_evidence_consistency(
    catalog: pd.DataFrame,
    classification: list[dict[str, Any]],
    announcements: list[dict[str, Any]],
    archive_rows: list[dict[str, Any]],
    first_trades: list[dict[str, Any]],
    scope_registry: dict[str, Any],
    noncanonical_candidates: list[str],
    announcement_audit: dict[str, Any],
    delisting_records: list[dict[str, Any]],
    candidate_identities: list[str] | None = None,
) -> None:
    archive_by_symbol = {row.get("symbol"): row for row in archive_rows}
    if len(archive_by_symbol) != len(archive_rows) or set(catalog["symbol"]) != set(
        archive_by_symbol
    ):
        raise ValueError("Archive observations do not match lifecycle catalog identities")
    classification_by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for item in classification:
        classification_by_key.setdefault(
            (item.get("contract_identity"), item.get("dimension")), []
        ).append(item)
    accepted_announcements = [
        item
        for item in announcements
        if item.get("match_status")
        in {"accepted", "ambiguous_multiple_applicable_articles"}
    ]
    candidates = sorted(
        candidate_identities
        if candidate_identities is not None
        else set(archive_by_symbol) | set(noncanonical_candidates)
    )
    scope_by_identity = verify_scope_registry(
        scope_registry, candidates, require_independent_review=False
    )
    expected_catalog_symbols = {
        symbol
        for symbol, scope in scope_by_identity.items()
        if scope.get("product_scope") in {"in_scope_crypto_perpetual", "benchmark_only"}
    }
    if not expected_catalog_symbols.issubset(archive_by_symbol):
        raise ValueError("Lifecycle catalog lacks an explicit row for a reviewed in-scope candidate")
    trade_by_symbol = {row.get("symbol"): row for row in first_trades}
    if len(trade_by_symbol) != len(first_trades):
        raise ValueError("First-observed-trade evidence contains duplicate identities")
    delisting_catalogs = [
        item
        for item in announcement_audit.get("catalogs", [])
        if item.get("event_type") == "delisting"
    ]
    if len(delisting_catalogs) != 1:
        raise ValueError("Delisting corpus audit identity is incomplete")
    delisting_audit = delisting_catalogs[0]
    if (
        delisting_audit.get("inspection_policy") != "complete_catalog_detail_inspection"
        or delisting_audit.get("candidate_articles") != delisting_audit.get("declared_total")
    ):
        raise ValueError("Delisting corpus was not inspected comprehensively")
    for row in catalog.to_dict("records"):
        symbol = row["symbol"]
        scope = scope_by_identity[symbol]
        for catalog_field, registry_field in (
            ("scope_disposition", "product_scope"),
            ("is_crypto_underlying", "is_crypto_underlying"),
            ("is_stablecoin_underlying", "is_stablecoin_underlying"),
            ("is_leveraged_token", "is_leveraged_token"),
        ):
            if row.get(catalog_field) != scope.get(registry_field):
                raise ValueError("Catalog scope classification disagrees with bound registry")
        archive = archive_by_symbol[symbol]
        observed = archive.get("observed_archive_object_keys")
        frontier_only = archive.get("archive_discovery_provenance") == (
            "frontier_daily_candidate_only_no_monthly_archive_blocking"
        )
        if frontier_only:
            if observed != [] or row.get("historical_inclusion_readiness") != "blocked":
                raise ValueError("Frontier-only archive rows must remain explicitly blocked")
        else:
            if type(observed) is not list or not observed:
                raise ValueError("Archive evidence lacks observed ZIP objects")
            identities = [validate_archive_object_key(key) for key in observed]
            if any(identity.symbol != symbol for identity in identities):
                raise ValueError("Archive evidence contains a mismatched object identity")
            periods = sorted(identity.period for identity in identities)
            if row.get("first_archive_month") != periods[0] or row.get("last_archive_month") != periods[-1]:
                raise ValueError("Catalog archive bounds disagree with observed ZIP evidence")
        if row.get("scope_classification_complete") is True:
            for dimension, field in (
                ("stablecoin_underlying", "is_stablecoin_underlying"),
                ("leveraged_token", "is_leveraged_token"),
            ):
                if type(row.get(field)) is not bool:
                    continue
                matching = [
                    item
                    for item in classification_by_key.get((symbol, dimension), [])
                    if item.get("value") is row.get(field)
                    and item.get("conflict_status") == "none"
                    and item.get("evidence_status")
                    not in {None, "unresolved_no_affirmative_negative_evidence"}
                ]
                if len(matching) != 1:
                    raise ValueError("Resolved catalog classification lacks exact evidence")
        if row.get("official_trading_start_at") is not None:
            matching = [
                item
                for item in accepted_announcements
                if item.get("event_type") == "listing"
                and item.get("symbol") == symbol
                and item.get("article_code") == row.get("listing_article_id")
                and item.get("official_event_at") == row.get("official_trading_start_at")
            ]
            if len(matching) != 1:
                raise ValueError("Catalog listing start lacks exact accepted announcement evidence")
        trade = trade_by_symbol.get(symbol)
        if row.get("first_observed_trade_at") is not None:
            if trade is None or trade.get("earliest_trade_timestamp") != row.get(
                "first_observed_trade_at"
            ):
                raise ValueError("Catalog observed-trade anchor lacks exact primitive evidence")
            if trade.get("computed_sha256") != trade.get("published_sha256"):
                raise ValueError("First-observed-trade checksum evidence is inconsistent")
        basis = row.get("eligibility_age_anchor_basis")
        if basis == "exact_official_original_launch" and row.get(
            "eligibility_age_anchor_at"
        ) != row.get("exact_official_trading_start_at"):
            raise ValueError("Exact launch age anchor is not primitive-derived")
        if basis in {
            "first_observed_binance_futures_trade",
            "legacy_pre_research_start_adjudicated",
        } and row.get("eligibility_age_anchor_at") != row.get("first_observed_trade_at"):
            raise ValueError("Observed-trade age anchor is not primitive-derived")
        if row.get("delisting_announcement_published_at") is not None:
            matching = [
                item
                for item in accepted_announcements
                if item.get("event_type") == "delisting"
                and item.get("symbol") == symbol
                and item.get("article_code") == row.get("delisting_article_id")
                and item.get("article_published_at")
                == row.get("delisting_announcement_published_at")
            ]
            if len(matching) != 1:
                raise ValueError("Catalog delisting cutoff lacks accepted announcement evidence")
        for cutoff in delisting_records:
            if cutoff.get("symbol") != symbol or cutoff.get("review_status") != "accepted_exact_cutoff":
                continue
            matching = [
                item
                for item in accepted_announcements
                if item.get("event_type") == "delisting"
                and item.get("symbol") == symbol
                and item.get("article_code") == cutoff.get("article_code")
            ]
            if len(matching) != 1 or matching[0].get("raw_snapshot_sha256") != cutoff.get(
                "raw_article_sha256"
            ):
                raise ValueError("Reviewed delisting cutoff does not bind the exact article detail")
            if matching[0].get("article_published_at") != cutoff.get(
                "official_publication_timestamp"
            ) or matching[0].get("official_event_at") != cutoff.get("terminal_last_trading_at"):
                raise ValueError("Reviewed delisting cutoff semantics disagree with article detail")


def verify_lifecycle_bundle(bundle_path: str | Path) -> dict[str, Any]:
    path = Path(bundle_path).resolve()
    bundle = json.loads(path.read_text(encoding="utf-8"))
    if type(bundle) is not dict or bundle.get("schema_version") != BUNDLE_SCHEMA_VERSION:
        raise ValueError("Lifecycle bundle has an unsupported schema")
    if bundle.get("purpose") != "content_bound_lifecycle_authorization":
        raise ValueError("Lifecycle bundle has the wrong purpose")
    core = {key: value for key, value in bundle.items() if key != "bundle_id"}
    if bundle.get("bundle_id") != content_identity(core):
        raise ValueError("Lifecycle bundle identity is invalid")
    required = {
        "lifecycle_catalog.json",
        "coverage.json",
        "classification_evidence.json",
        "announcement_evidence.json",
        "archive_observations.json",
        "noncanonical_archive_prefix_queue.json",
        "readiness.json",
        "historical_scope_registry.json",
        "first_observed_trades.json",
        "announcement_corpus_audit.json",
        "candidate_inventory.json",
        "lifecycle_adjudications.json",
        "lifecycle_daily_trade_boundaries.json",
        "primitive_evidence_manifest.json",
        "episode_first_observed_trades.json",
        "lifecycle_freshness.json",
        "historical_delisting_cutoff_registry.json",
        "delisting_registry_independent_review.json",
        "independent_eligibility_verification_report.json",
    }
    artifacts = bundle.get("artifacts")
    if type(artifacts) is not dict or set(artifacts) != required:
        raise ValueError("Lifecycle bundle artifact set is incomplete or unexpected")
    resolved: dict[str, Path] = {}
    for name, descriptor in artifacts.items():
        if type(descriptor) is not dict or descriptor.get("path") != name:
            raise ValueError("Lifecycle bundle artifact path is noncanonical")
        artifact_path = (path.parent / name).resolve()
        if artifact_path.parent != path.parent:
            raise ValueError("Lifecycle bundle artifact escapes its report directory")
        if sha256_path(artifact_path) != descriptor.get("sha256"):
            raise ValueError(f"Lifecycle bundle artifact hash mismatch: {name}")
        resolved[name] = artifact_path
    config = bundle.get("config")
    if type(config) is not dict:
        raise ValueError("Lifecycle bundle config identity is missing")
    config_path = Path(config.get("path", ""))
    if not config_path.is_absolute() or sha256_path(config_path) != config.get("sha256"):
        raise ValueError("Lifecycle bundle config digest is invalid")
    catalog = pd.DataFrame(json.loads(resolved["lifecycle_catalog.json"].read_text("utf-8")))
    if catalog.empty or not catalog["catalog_schema_version"].eq(CATALOG_SCHEMA_VERSION).all():
        raise ValueError("Lifecycle catalog schema identity is invalid")
    classification = json.loads(resolved["classification_evidence.json"].read_text("utf-8"))
    announcements = json.loads(resolved["announcement_evidence.json"].read_text("utf-8"))
    archive_rows = json.loads(resolved["archive_observations.json"].read_text("utf-8"))
    first_trades = json.loads(resolved["first_observed_trades.json"].read_text("utf-8"))
    scope_registry = json.loads(
        resolved["historical_scope_registry.json"].read_text("utf-8")
    )
    announcement_audit = json.loads(
        resolved["announcement_corpus_audit.json"].read_text("utf-8")
    )
    if not all(
        type(value) is list
        for value in (classification, announcements, archive_rows, first_trades)
    ):
        raise ValueError("Lifecycle evidence artifacts must contain record lists")
    noncanonical = json.loads(
        resolved["noncanonical_archive_prefix_queue.json"].read_text("utf-8")
    )
    if noncanonical.get("status") != "reviewed_finite_universe" or type(
        noncanonical.get("prefixes")
    ) is not list:
        raise ValueError("Noncanonical archive queue is malformed")
    inventory = json.loads(resolved["candidate_inventory.json"].read_text("utf-8"))
    candidates = inventory.get("candidate_identities")
    if type(candidates) is not list:
        raise ValueError("Candidate inventory identities are malformed")
    verify_scope_registry(
        scope_registry,
        candidates,
        registry_path=resolved["historical_scope_registry.json"],
        repository_root=config_path.parent.parent,
    )
    primitive = json.loads(
        resolved["primitive_evidence_manifest.json"].read_text("utf-8")
    )
    replay = json.loads(
        resolved["independent_eligibility_verification_report.json"].read_text("utf-8")
    )
    delisting = json.loads(
        resolved["historical_delisting_cutoff_registry.json"].read_text("utf-8")
    )
    delisting_review = json.loads(
        resolved["delisting_registry_independent_review.json"].read_text("utf-8")
    )
    episodes = json.loads(
        resolved["episode_first_observed_trades.json"].read_text("utf-8")
    )
    freshness = json.loads(resolved["lifecycle_freshness.json"].read_text("utf-8"))
    validate_lifecycle_freshness(freshness, report_root=path.parent)
    if bundle.get("lifecycle_evidence_valid_through_utc") != freshness[
        "lifecycle_evidence_valid_through_utc"
    ]:
        raise ValueError("Lifecycle bundle horizon disagrees with lifecycle freshness")
    observed_corpus = cms_corpus_binding(announcement_audit)
    if freshness["announcement_corpus"]["identity"] != observed_corpus["identity"]:
        raise ValueError("Lifecycle freshness does not bind the complete CMS corpus")
    observed_started = pd.Timestamp(announcement_audit.get("rebuild_started_at"))
    if observed_started.tzinfo is None:
        observed_started = observed_started.tz_localize("UTC")
    else:
        observed_started = observed_started.tz_convert("UTC")
    if freshness["announcement_corpus"]["acquisition_started_at_utc"] != observed_started.isoformat().replace(
        "+00:00", "Z"
    ):
        raise ValueError("Lifecycle freshness CMS acquisition-start evidence is stale")
    adjudications = json.loads(resolved["lifecycle_adjudications.json"].read_text("utf-8"))
    primitive_core = {key: value for key, value in primitive.items() if key != "manifest_id"}
    replay_core = {
        key: value for key, value in replay.items() if key != "verification_report_id"
    }
    adjudication_core = {
        key: value for key, value in adjudications.items() if key != "adjudication_id"
    }
    chain = bundle.get("authorization_chain")
    expected_chain = {
        "primitive_evidence_manifest_id": primitive.get("manifest_id"),
        "primitive_evidence_manifest_sha256": artifacts[
            "primitive_evidence_manifest.json"
        ]["sha256"],
        "eligibility_oracle_report_id": replay.get("verification_report_id"),
        "eligibility_oracle_report_sha256": artifacts[
            "independent_eligibility_verification_report.json"
        ]["sha256"],
        "reviewed_scope_registry_id": scope_registry.get("registry_id"),
        "reviewed_scope_registry_sha256": artifacts[
            "historical_scope_registry.json"
        ]["sha256"],
        "scope_independent_review": scope_registry.get("independent_review"),
        "lifecycle_adjudication_id": adjudications.get("adjudication_id"),
        "reviewed_delisting_registry_id": delisting.get("registry_id"),
        "reviewed_delisting_registry_sha256": artifacts[
            "historical_delisting_cutoff_registry.json"
        ]["sha256"],
        "delisting_independent_review_id": delisting_review.get("review_id"),
        "delisting_independent_review_sha256": artifacts[
            "delisting_registry_independent_review.json"
        ]["sha256"],
        "episode_boundary_evidence_id": episodes.get("evidence_id"),
        "episode_boundary_evidence_sha256": artifacts[
            "episode_first_observed_trades.json"
        ]["sha256"],
        "lifecycle_freshness_id": freshness.get("freshness_id"),
        "lifecycle_freshness_sha256": artifacts["lifecycle_freshness.json"]["sha256"],
    }
    expected_replay = run_eligibility_oracle(
        path.parent,
        repository_root=config_path.parent.parent,
        config_path=config_path,
        executable_commit=bundle.get("code_commit"),
    )
    if (
        primitive.get("manifest_id") != content_identity(primitive_core)
        or replay.get("verification_report_id") != content_identity(replay_core)
        or replay != expected_replay
        or replay.get("oracle_verification_status") != "PASS"
        or replay.get("final_status") != "PASS"
        or adjudications.get("adjudication_id") != content_identity(adjudication_core)
        or inventory.get("candidate_set_digest") != scope_registry.get("candidate_set_digest")
        or inventory.get("candidate_set_digest") != candidate_set_digest(
            inventory.get("candidate_identities", [])
        )
        or inventory.get("inventory_id") != content_identity(
            {key: value for key, value in inventory.items() if key != "inventory_id"}
        )
        or chain != expected_chain
    ):
        raise ValueError("Lifecycle authorization chain is invalid")
    from alt_hot_scanner.universe.evidence_replay import run_full_evidence_replay

    replay_adjudications = load_lifecycle_adjudications(
        resolved["lifecycle_adjudications.json"],
        candidate_set_digest=inventory["candidate_set_digest"],
    )
    full_replay = run_full_evidence_replay(
        path.parent,
        repository_root=config_path.parent.parent,
        lifecycle_adjudications=replay_adjudications,
    )
    if type(full_replay) is not dict or full_replay.get("status") != "PASS":
        raise ValueError("Canonical full lifecycle evidence replay did not pass")
    _verify_catalog_evidence_consistency(
        catalog,
        classification,
        announcements,
        archive_rows,
        first_trades,
        scope_registry,
        noncanonical["prefixes"],
        announcement_audit,
        delisting.get("records", []),
        candidate_identities=candidates,
    )
    unresolved_noncanonical = [
        identity
        for identity in noncanonical["prefixes"]
        if next(
            record
            for record in scope_registry["records"]
            if record["contract_identity"] == identity
        )["scope_audit_status"]
        != "complete"
    ]
    recomputed = catalog_readiness(catalog, unresolved_noncanonical)
    stored_readiness = json.loads(resolved["readiness.json"].read_text("utf-8"))
    if recomputed != stored_readiness:
        raise ValueError("Stored lifecycle readiness disagrees with recomputed evidence")
    coverage = json.loads(resolved["coverage.json"].read_text("utf-8"))
    if coverage.get("recomputed_readiness") != recomputed:
        raise ValueError("Coverage report disagrees with recomputed lifecycle readiness")
    return {
        "bundle": bundle,
        "bundle_path": path,
        "artifacts": resolved,
        "config_path": config_path,
        "catalog": catalog,
        "readiness": recomputed,
    }


def build_plan_integrity(plan: dict[str, Any]) -> str:
    return content_identity({key: value for key, value in plan.items() if key != "plan_integrity"})


def verify_bound_plan(plan_path: str | Path) -> dict[str, Any]:
    path = Path(plan_path).resolve()
    plan = json.loads(path.read_text(encoding="utf-8"))
    if type(plan) is not dict or plan.get("schema_version") != PLAN_SCHEMA_VERSION:
        raise ValueError("Download plan has an unsupported schema")
    if set(plan) != PLAN_FIELDS:
        raise ValueError("Download plan fields do not match the exact schema")
    if plan.get("purpose") != "authorized_full_history_archive_download":
        raise ValueError("Download plan has the wrong purpose")
    if plan.get("plan_integrity") != build_plan_integrity(plan):
        raise ValueError("Download plan integrity is invalid")
    bundle_path = Path(plan.get("lifecycle_bundle", ""))
    if not bundle_path.is_absolute():
        raise ValueError("Download plan lifecycle bundle path must be absolute")
    verified = verify_lifecycle_bundle(bundle_path)
    bundle = verified["bundle"]
    if plan.get("lifecycle_bundle_id") != bundle["bundle_id"]:
        raise ValueError("Download plan references the wrong lifecycle bundle")
    approval_path = Path(plan.get("lifecycle_approval", ""))
    if not approval_path.is_absolute():
        raise ValueError("Download plan lifecycle approval path must be absolute")
    verified_approval = verify_approval_pin(approval_path, verified)
    if plan.get("lifecycle_approval_id") != verified_approval["approval"]["approval_id"]:
        raise ValueError("Download plan references the wrong lifecycle approval")
    if plan.get("approval_state_registry") != {
        "path": str(verified_approval["state_registry_path"].resolve()),
        "sha256": verified_approval["state_registry_sha256"],
        "registry_id": verified_approval["state_registry"]["registry_id"],
    }:
        raise ValueError("Download plan approval state is stale or changed")
    if plan.get("config_sha256") != bundle["config"]["sha256"]:
        raise ValueError("Download plan references the wrong config digest")
    if plan.get("artifact_hashes") != {
        name: descriptor["sha256"] for name, descriptor in bundle["artifacts"].items()
    }:
        raise ValueError("Download plan artifact hashes disagree with the lifecycle bundle")
    if not verified["readiness"]["authorization_ready"]:
        raise ValueError("Lifecycle readiness is blocked; execution is prohibited")
    if plan.get("readiness") != verified["readiness"]:
        raise ValueError("Download plan readiness disagrees with the lifecycle bundle")
    if plan.get("planning_basis") != "exact_observed_monthly_zip_objects":
        raise ValueError("Download plan does not use observed ZIP objects")
    if type(plan.get("objects")) is not list or not plan["objects"]:
        raise ValueError("Download plan must contain a nonempty objects list")
    config = load_config(verified["config_path"])
    if plan.get("source") != config["data"]["archive_index_url"]:
        raise ValueError("Download plan source disagrees with the bound config")
    catalog = verified["catalog"].copy()
    catalog["underlying_subtype"] = catalog["underlying_subtype"].map(
        lambda value: tuple(json.loads(value)) if isinstance(value, str) else value
    )
    scoped = filter_instrument_scope(catalog, config["universe"]["stablecoin_underlyings"])
    approved_symbols = set(
        scoped.loc[scoped["historical_inclusion_readiness"].eq("ready"), "symbol"]
    )
    if plan.get("symbols_discovered") != len(approved_symbols):
        raise ValueError("Download plan approved-symbol count is invalid")
    start_month = pd.Timestamp(config["data"]["start"]).tz_localize(None).to_period("M") - 1
    if plan.get("warmup_start_month") != str(start_month):
        raise ValueError("Download plan warmup boundary is invalid")
    try:
        end_month = pd.Period(plan.get("end_month"), freq="M")
    except (TypeError, ValueError) as exc:
        raise ValueError("Download plan end month is invalid") from exc
    archive_rows = json.loads(
        verified["artifacts"]["archive_observations.json"].read_text(encoding="utf-8")
    )
    expected: set[str] = set()
    for row in archive_rows:
        if row.get("symbol") not in approved_symbols:
            continue
        observed = row.get("observed_archive_object_keys")
        if type(observed) is not list:
            raise ValueError("Archive observations lack exact observed ZIP keys")
        for key in observed:
            identity = validate_archive_object_key(key)
            if identity.symbol != row["symbol"]:
                raise ValueError("Archive observation symbol does not match its ZIP key")
            period = pd.Period(identity.period, freq="M")
            if start_month <= period <= end_month:
                expected.add(identity.object_key)
    supplied = [validate_archive_object_key(key).object_key for key in plan["objects"]]
    if len(supplied) != len(set(supplied)) or set(supplied) != expected:
        raise ValueError("Download plan objects disagree with exact observed approved ZIP objects")
    return {
        "plan": plan,
        "verified_bundle": verified,
        "verified_approval": verified_approval,
    }
