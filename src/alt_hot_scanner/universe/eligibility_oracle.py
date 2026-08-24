from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import zipfile
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd

ORACLE_REPORT_SCHEMA_VERSION = "independent-eligibility-oracle-report-v1"
EPISODE_EVIDENCE_SCHEMA_VERSION = "lifecycle-episode-first-trade-evidence-v1"
_DAILY_KEY = re.compile(
    r"data/futures/um/daily/trades/(?P<symbol>[^/\\]+)/"
    r"(?P=symbol)-trades-(?P<date>20\d\d-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01]))\.zip"
)


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def content_identity(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def sha256_path(path: str | Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _load(path: Path) -> Any:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ValueError(f"Duplicate JSON key: {key}")
            result[key] = value
        return result

    return json.loads(path.read_bytes(), object_pairs_hook=pairs)


def _iso(value: object) -> str | None:
    if value is None:
        return None
    return pd.Timestamp(value).isoformat()


def _cms_binding(audit: dict[str, Any]) -> dict[str, Any]:
    catalogs = [
        item for item in audit.get("catalogs", []) if item.get("event_type") == "delisting"
    ]
    if len(catalogs) != 1:
        raise ValueError("Oracle requires one exact delisting CMS catalog")
    catalog = catalogs[0]
    stable = {
        "schema_version": "binance-delisting-cms-corpus-v1",
        "parser_version": audit.get("parser_version"),
        "catalog_id": catalog.get("catalog_id"),
        "declared_total": catalog.get("declared_total"),
        "pages": catalog.get("pages"),
        "candidate_articles": catalog.get("candidate_articles"),
        "inspection_policy": catalog.get("inspection_policy"),
        "page_sha256s": catalog.get("page_sha256s"),
    }
    digest = content_identity(stable)
    return {"identity": digest, "sha256": digest, "delisting_catalog_id": catalog["catalog_id"]}


def _earliest_trade(path: Path, expected_member: str) -> str:
    with zipfile.ZipFile(path) as archive:
        members = [item for item in archive.infolist() if not item.is_dir()]
        if len(members) != 1 or members[0].filename != expected_member:
            raise ValueError("Oracle trade archive must contain exactly one CSV")
        minimum: int | None = None
        with archive.open(members[0]) as raw:
            reader = csv.reader(io.TextIOWrapper(raw, encoding="utf-8-sig", newline=""))
            for position, row in enumerate(reader):
                if position == 0 and row and row[0].strip().lower() == "id":
                    continue
                if len(row) < 5 or not row[4].isdigit():
                    raise ValueError("Oracle trade archive contains an invalid timestamp")
                value = int(row[4])
                minimum = value if minimum is None else min(minimum, value)
    if minimum is None:
        raise ValueError("Oracle trade archive contains no trades")
    return pd.to_datetime(minimum, unit="ms", utc=True).isoformat()


def derive_expected_eligibility(report_root: str | Path) -> dict[str, Any]:
    """Independently reconstruct eligibility-critical episode state from reviewed inputs."""
    root = Path(report_root).resolve(strict=True)
    inventory = _load(root / "candidate_inventory.json")
    scope = _load(root / "historical_scope_registry.json")
    adjudications = _load(root / "lifecycle_adjudications.json")
    episode_evidence = _load(root / "episode_first_observed_trades.json")
    delisting = _load(root / "historical_delisting_cutoff_registry.json")
    boundaries = _load(root / "lifecycle_daily_trade_boundaries.json")
    primitive = _load(root / "primitive_evidence_manifest.json")
    announcement_audit = _load(root / "announcement_corpus_audit.json")

    evidence_core = {
        key: value for key, value in episode_evidence.items() if key != "evidence_id"
    }
    if (
        episode_evidence.get("schema_version") != EPISODE_EVIDENCE_SCHEMA_VERSION
        or episode_evidence.get("evidence_id") != content_identity(evidence_core)
        or episode_evidence.get("candidate_set_digest") != inventory["candidate_set_digest"]
    ):
        raise ValueError("Episode first-trade evidence identity is invalid")
    registry_core = {key: value for key, value in delisting.items() if key != "registry_id"}
    if (
        delisting.get("registry_id") != content_identity(registry_core)
        or delisting.get("candidate_set_digest") != inventory["candidate_set_digest"]
    ):
        raise ValueError("Reviewed delisting registry identity is invalid")
    if delisting.get("official_cms_corpus") != _cms_binding(announcement_audit):
        raise ValueError("Reviewed delisting registry targets a stale CMS corpus")
    scope_by_symbol = {row["contract_identity"]: row for row in scope["records"]}
    adj_by_symbol = {row["symbol"]: row for row in adjudications["records"]}
    trade_by_episode = {
        row["lifecycle_episode_id"]: row for row in episode_evidence["records"]
    }
    cutoff_by_episode = {
        row["lifecycle_episode_id"]: row for row in delisting["records"]
    }
    primitive_by_path = {str(Path(row["path"]).resolve()): row for row in primitive["entries"]}
    boundary_by_symbol = {row["symbol"]: row for row in boundaries}
    if len(trade_by_episode) != len(episode_evidence["records"]):
        raise ValueError("Episode first-trade evidence contains duplicate episodes")
    if len(cutoff_by_episode) != len(delisting["records"]):
        raise ValueError("Delisting registry contains duplicate episodes")

    expected: list[dict[str, Any]] = []
    blockers: set[str] = set()
    in_scope = [
        symbol
        for symbol, row in scope_by_symbol.items()
        if row.get("product_scope") in {"in_scope_crypto_perpetual", "benchmark_only"}
    ]
    for symbol in sorted(in_scope):
        adjudication = adj_by_symbol.get(symbol)
        specs = adjudication["episodes"] if adjudication else [{"episode_id": f"{symbol}:1"}]
        boundary = boundary_by_symbol.get(symbol)
        for position, spec in enumerate(specs):
            episode_id = spec["episode_id"]
            trade = trade_by_episode.get(episode_id)
            cutoff = cutoff_by_episode.get(episode_id)
            conflict = None
            if (
                trade is None
                or trade.get("symbol") != symbol
                or trade.get("published_sha256") != trade.get("computed_sha256")
                or trade.get("evidence_status")
                != "checksum_verified_official_binance_futures_trade"
            ):
                conflict = "missing_or_invalid_verified_first_trade"
            elif (
                (key_match := _DAILY_KEY.fullmatch(str(trade.get("archive_object_key")))) is None
                or key_match.group("symbol") != symbol
                or key_match.group("date") != trade.get("archive_date")
                or str(Path(trade["raw_path"]).resolve()) not in primitive_by_path
                or sha256_path(trade["raw_path"]) != trade["published_sha256"]
                or primitive_by_path[str(Path(trade["raw_path"]).resolve())].get(
                    "evidence_role"
                )
                not in {"first_observed_trade_zip", "episode_first_observed_trade_zip"}
                or primitive_by_path[str(Path(trade["raw_path"]).resolve())].get(
                    "source_identifier"
                )
                != trade["archive_object_key"]
                or primitive_by_path[str(Path(trade["raw_path"]).resolve())].get("sha256")
                != trade["published_sha256"]
                or _earliest_trade(
                    Path(trade["raw_path"]),
                    f"{symbol}-trades-{trade['archive_date']}.csv",
                )
                != _iso(trade["earliest_trade_timestamp"])
            ):
                conflict = "episode_trade_primitive_verification_failed"
            if cutoff is None:
                conflict = conflict or "missing_reviewed_delisting_disposition"
            elif cutoff.get("review_status") == "unresolved_conflicting_evidence":
                conflict = conflict or "unresolved_delisting_evidence"
            anchor = None if trade is None else _iso(trade.get("earliest_trade_timestamp"))
            eligible_from = (
                None
                if anchor is None
                else (pd.Timestamp(anchor).to_pydatetime() + timedelta(days=30)).isoformat()
            )
            terminal = _iso(spec.get("terminated_at"))
            if terminal is None and cutoff is not None:
                terminal = _iso(cutoff.get("terminal_last_trading_at"))
            if position > 0:
                gap = adjudication.get("gap_evidence", {})
                expected_date = gap.get("first_post_gap_trade_archive_date")
                if expected_date is None or trade is None or trade.get("archive_date") != expected_date:
                    conflict = conflict or "post_gap_trade_archive_mismatch"
                if boundary is None or expected_date not in boundary.get(
                    "observed_archive_dates", []
                ):
                    conflict = conflict or "post_gap_boundary_not_observed"
            if conflict:
                blockers.add(symbol)
            expected.append(
                {
                    "symbol": symbol,
                    "scope_disposition": scope_by_symbol[symbol]["product_scope"],
                    "lifecycle_episode_id": episode_id,
                    "age_live_anchor_at": anchor,
                    "anchor_basis": "first_observed_binance_futures_trade",
                    "eligible_from": eligible_from,
                    "delisting_announcement_published_at": (
                        None if cutoff is None else _iso(cutoff.get("official_publication_timestamp"))
                    ),
                    "delisting_article_id": None if cutoff is None else cutoff.get("article_code"),
                    "last_trading_at": terminal,
                    "eligibility_end_at": (
                        None
                        if cutoff is None and terminal is None
                        else _iso(cutoff.get("official_publication_timestamp"))
                        if cutoff is not None
                        and cutoff.get("official_publication_timestamp") is not None
                        else terminal
                    ),
                    "conflict": conflict,
                }
            )
    return {
        "candidate_set_digest": inventory["candidate_set_digest"],
        "candidate_count": len(inventory["candidate_identities"]),
        "in_scope_count": len(in_scope),
        "episode_count": len(expected),
        "ready_count": len(in_scope) - len(blockers),
        "blocker_count": len(blockers),
        "blocker_symbols": sorted(blockers),
        "episodes": expected,
    }


def compare_production_catalog(expected: dict[str, Any], catalog: list[dict[str, Any]]) -> None:
    by_symbol = {row["symbol"]: row for row in catalog}
    expected_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for episode in expected["episodes"]:
        expected_by_symbol.setdefault(episode["symbol"], []).append(episode)
    for symbol, episodes in expected_by_symbol.items():
        row = by_symbol.get(symbol)
        if row is None or row.get("scope_disposition") != episodes[0]["scope_disposition"]:
            raise ValueError(f"Oracle scope mismatch for {symbol}")
        actual = row.get("lifecycle_intervals")
        if type(actual) is str:
            actual = json.loads(actual)
        if type(actual) is not list or len(actual) != len(episodes):
            raise ValueError(f"Oracle episode-count mismatch for {symbol}")
        for expected_episode, actual_episode in zip(episodes, actual, strict=True):
            for field in (
                "lifecycle_episode_id",
                "age_live_anchor_at",
                "anchor_basis",
                "eligible_from",
                "delisting_announcement_published_at",
                "delisting_article_id",
                "last_trading_at",
                "eligibility_end_at",
            ):
                actual_value = actual_episode.get(field)
                expected_value = expected_episode.get(field)
                if field in {
                    "age_live_anchor_at",
                    "eligible_from",
                    "delisting_announcement_published_at",
                    "last_trading_at",
                    "eligibility_end_at",
                }:
                    actual_value = _iso(actual_value)
                    expected_value = _iso(expected_value)
                if actual_value != expected_value:
                    raise ValueError(
                        f"Oracle {field} mismatch for {expected_episode['lifecycle_episode_id']}"
                    )
            expected_status = "blocked" if expected_episode["conflict"] else "reviewed_resolved"
            if actual_episode.get("interval_evidence_status") != expected_status:
                raise ValueError(
                    f"Oracle conflict/readiness mismatch for {expected_episode['lifecycle_episode_id']}"
                )
        expected_ready = "blocked" if any(item["conflict"] for item in episodes) else "ready"
        if row.get("historical_inclusion_readiness") != expected_ready:
            raise ValueError(f"Oracle historical readiness mismatch for {symbol}")


def run_eligibility_oracle(
    report_root: str | Path,
    *,
    repository_root: str | Path,
    config_path: str | Path,
    executable_commit: str,
) -> dict[str, Any]:
    root = Path(report_root).resolve(strict=True)
    repository = Path(repository_root).resolve(strict=True)
    expected = derive_expected_eligibility(root)
    catalog_path = root / "lifecycle_catalog.json"
    catalog = _load(catalog_path)
    compare_production_catalog(expected, catalog)
    scope = _load(root / "historical_scope_registry.json")
    scope_review_path = repository / scope["independent_review"]["path"]
    delisting = _load(root / "historical_delisting_cutoff_registry.json")
    delisting_review = _load(root / "delisting_registry_independent_review.json")
    primitive = _load(root / "primitive_evidence_manifest.json")
    adjudications = _load(root / "lifecycle_adjudications.json")
    episode_evidence = _load(root / "episode_first_observed_trades.json")
    review_core = {
        key: value for key, value in delisting_review.items() if key != "review_id"
    }
    if (
        delisting_review.get("schema_version")
        != "historical-delisting-cutoff-review-v1"
        or delisting_review.get("review_id") != content_identity(review_core)
        or delisting_review.get("verdict") != "PASS"
        or delisting_review.get("registry_id") != delisting["registry_id"]
        or delisting_review.get("registry_sha256")
        != sha256_path(root / "historical_delisting_cutoff_registry.json")
        or delisting_review.get("reviewed_episode_count") != len(delisting["records"])
    ):
        raise ValueError("Delisting registry independent review is invalid")
    core = {
        "schema_version": ORACLE_REPORT_SCHEMA_VERSION,
        "primitive_manifest_id": primitive["manifest_id"],
        "primitive_manifest_sha256": sha256_path(root / "primitive_evidence_manifest.json"),
        "candidate_set_digest": expected["candidate_set_digest"],
        "candidate_count": expected["candidate_count"],
        "reviewed_scope_registry_id": scope["registry_id"],
        "reviewed_scope_registry_sha256": sha256_path(
            root / "historical_scope_registry.json"
        ),
        "scope_independent_review_id": scope["independent_review"]["identifier"],
        "scope_independent_review_sha256": sha256_path(scope_review_path),
        "reviewed_delisting_registry_id": delisting["registry_id"],
        "reviewed_delisting_registry_sha256": sha256_path(
            root / "historical_delisting_cutoff_registry.json"
        ),
        "delisting_independent_review_id": delisting_review["review_id"],
        "delisting_independent_review_sha256": sha256_path(
            root / "delisting_registry_independent_review.json"
        ),
        "lifecycle_adjudication_id": adjudications["adjudication_id"],
        "episode_boundary_evidence_id": episode_evidence["evidence_id"],
        "episode_boundary_evidence_sha256": sha256_path(
            root / "episode_first_observed_trades.json"
        ),
        "config_sha256": sha256_path(config_path),
        "executable_lifecycle_commit": executable_commit,
        "production_catalog_sha256": sha256_path(catalog_path),
        "catalog_row_count": len(catalog),
        "independently_derived_in_scope_count": expected["in_scope_count"],
        "independently_derived_episode_count": expected["episode_count"],
        "independently_derived_ready_count": expected["ready_count"],
        "independently_derived_blocker_count": expected["blocker_count"],
        "independently_derived_blocker_symbols": expected["blocker_symbols"],
        "oracle_verification_status": "PASS",
        "final_status": "PASS" if expected["blocker_count"] == 0 else "FAIL",
    }
    return {**core, "verification_report_id": content_identity(core)}
