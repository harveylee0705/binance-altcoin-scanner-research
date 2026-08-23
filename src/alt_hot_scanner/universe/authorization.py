from __future__ import annotations

import hashlib
import json
import subprocess
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any

import pandas as pd

from alt_hot_scanner.data.binance_public import (
    validate_archive_object_key,
    verify_first_observed_trade_record,
)
from alt_hot_scanner.universe.contracts import filter_instrument_scope
from alt_hot_scanner.universe.lifecycle import CATALOG_SCHEMA_VERSION
from alt_hot_scanner.universe.scope_registry import verify_scope_registry
from alt_hot_scanner.utils.config import load_config

BUNDLE_SCHEMA_VERSION = "lifecycle-authorization-bundle-v2"
APPROVAL_SCHEMA_VERSION = "lifecycle-approval-pin-v1"
PLAN_SCHEMA_VERSION = "full-history-download-plan-v3"
PLAN_FIELDS = {
    "schema_version",
    "created_at",
    "purpose",
    "source",
    "lifecycle_bundle",
    "lifecycle_bundle_id",
    "lifecycle_approval",
    "lifecycle_approval_id",
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
        & catalog["age_anchor_conflict_status"].eq("none")
        & catalog["delisting_evidence_state"].isin(
            [
                "not_applicable_currently_trading",
                "exact_applicable_announcement_publication",
                "official_search_completed_no_reliable_announcement_timestamp",
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
    artifacts = {
        name: {"path": name, "sha256": sha256_path(report_root / name)}
        for name in sorted(artifact_names)
    }
    core = {
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "purpose": "content_bound_lifecycle_authorization",
        "created_at": created_at,
        "catalog_schema_version": CATALOG_SCHEMA_VERSION,
        "artifacts": artifacts,
        "config": {
            "path": str(config_path.resolve()),
            "sha256": sha256_path(config_path),
        },
        "code_commit": code_commit,
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
    if approval.get("approval_status") != "approved":
        raise ValueError("Lifecycle approval is not approved")
    review = approval.get("independent_review_artifact")
    if type(review) is not dict or set(review) != {"identifier", "path", "sha256"}:
        raise ValueError("Lifecycle approval review artifact is malformed")
    review_path = Path(review["path"])
    if not review_path.is_absolute() or sha256_path(review_path) != review["sha256"]:
        raise ValueError("Lifecycle approval review artifact digest is invalid")
    return {"approval": approval, "approval_path": path}


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


def _verify_catalog_evidence_consistency(
    catalog: pd.DataFrame,
    classification: list[dict[str, Any]],
    announcements: list[dict[str, Any]],
    archive_rows: list[dict[str, Any]],
    first_trades: list[dict[str, Any]],
    scope_registry: dict[str, Any],
    noncanonical_candidates: list[str],
    announcement_audit: dict[str, Any],
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
        item for item in announcements if item.get("match_status") == "accepted"
    ]
    candidates = sorted(set(archive_by_symbol) | set(noncanonical_candidates))
    scope_by_identity = verify_scope_registry(scope_registry, candidates)
    trade_by_symbol = {row.get("symbol"): row for row in first_trades}
    if len(trade_by_symbol) != len(first_trades):
        raise ValueError("First-observed-trade evidence contains duplicate identities")
    if first_trades:
        with ProcessPoolExecutor(max_workers=min(8, len(first_trades))) as executor:
            list(executor.map(verify_first_observed_trade_record, first_trades))
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
    _verify_catalog_evidence_consistency(
        catalog,
        classification,
        announcements,
        archive_rows,
        first_trades,
        scope_registry,
        noncanonical["prefixes"],
        announcement_audit,
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
