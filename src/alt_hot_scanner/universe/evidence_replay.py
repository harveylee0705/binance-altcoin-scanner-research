from __future__ import annotations

import hashlib
import json
import re
import xml.etree.ElementTree as ET
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd

from alt_hot_scanner.data.announcements import (
    ANNOUNCEMENT_PARSER_VERSION,
    DELISTING_CATALOG_ID,
    LISTING_CATALOG_ID,
    parse_announcement_evidence,
)
from alt_hot_scanner.data.binance_public import (
    observed_zip_keys_from_index_snapshots,
    sha256_file,
    verify_first_observed_trade_record,
)
from alt_hot_scanner.identity import require_binance_token
from alt_hot_scanner.universe.adjudications import verify_boundary_index
from alt_hot_scanner.universe.contracts import records_from_exchange_info
from alt_hot_scanner.universe.lifecycle import build_lifecycle_catalog
from alt_hot_scanner.universe.scope_registry import (
    candidate_set_digest,
    verify_scope_registry,
)

PRIMITIVE_MANIFEST_SCHEMA_VERSION = "lifecycle-primitive-evidence-manifest-v1"
FULL_REPLAY_SCHEMA_VERSION = "lifecycle-full-evidence-replay-v1"
_SHA256 = re.compile(r"[0-9a-f]{64}")


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def _identity(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _strict_json_bytes(payload: bytes) -> Any:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ValueError(f"Duplicate JSON key: {key}")
            result[key] = value
        return result

    return json.loads(payload, object_pairs_hook=pairs)


def build_primitive_evidence_manifest(
    raw_root: str | Path,
    *,
    first_trades: list[dict[str, Any]],
) -> dict[str, Any]:
    root = Path(raw_root).resolve(strict=True)
    trade_by_path = {str(Path(row["raw_path"]).resolve()): row for row in first_trades}
    entries: list[dict[str, Any]] = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = path.relative_to(root).as_posix()
        if relative == "archive_checkpoint.json" or relative.endswith(
            "trade_probe/first_observed_trades.json"
        ):
            continue
        if path.name.endswith(".provenance.json"):
            role = "provenance_sidecar"
            source_identifier = relative.removesuffix(".provenance.json")
            source_url = None
            retrieved_at = None
            parser_version = "raw-source-provenance-v1"
        else:
            provenance_path = path.with_name(f"{path.name}.provenance.json")
            provenance = (
                _strict_json_bytes(provenance_path.read_bytes())
                if provenance_path.exists()
                else None
            )
            trade = trade_by_path.get(str(path.resolve()))
            if relative.startswith("archive_index/symbols_page_"):
                role = "archive_candidate_index_xml"
            elif relative.startswith("archive_index/"):
                role = "archive_symbol_month_index_xml"
            elif relative == "exchange_info.json":
                role = "exchange_info_snapshot"
            elif relative.startswith("announcements/catalog_"):
                role = "cms_catalog_response"
            elif relative.startswith("announcements/article_"):
                role = "cms_article_detail_response"
            elif relative.startswith("trade_probe/first_observed_trades/"):
                role = "first_observed_trade_zip"
            elif relative.startswith("trade_probe/episode_first_observed_trades/"):
                role = "episode_first_observed_trade_zip"
            elif relative.startswith("daily_trade_boundary_index/"):
                role = "daily_trade_boundary_index_xml"
            else:
                continue
            source_identifier = trade["archive_object_key"] if trade else relative
            source_url = (
                provenance.get("url")
                if provenance
                else f"https://data.binance.vision/{trade['archive_object_key']}"
                if trade
                else None
            )
            retrieved_at = (
                provenance.get("original_retrieval_timestamp")
                if provenance
                else trade.get("original_retrieval_timestamp")
                if trade
                else None
            )
            parser_version = (
                provenance.get("acquisition_parser_version")
                if provenance
                else trade.get("parser_version")
                if trade
                else "lifecycle-primitive-unversioned-legacy"
            )
        entries.append(
            {
                "evidence_role": role,
                "path": str(path.resolve()),
                "sha256": sha256_file(path)[0],
                "source_url": source_url,
                "source_identifier": source_identifier,
                "original_retrieval_timestamp": retrieved_at,
                "parser_schema_version": parser_version,
            }
        )
    core = {
        "schema_version": PRIMITIVE_MANIFEST_SCHEMA_VERSION,
        "raw_root": str(root),
        "entries": entries,
    }
    return {**core, "manifest_id": _identity(core)}


def verify_primitive_manifest(path: str | Path) -> dict[str, Any]:
    source = Path(path).resolve(strict=True)
    manifest = _strict_json_bytes(source.read_bytes())
    if manifest.get("schema_version") != PRIMITIVE_MANIFEST_SCHEMA_VERSION:
        raise ValueError("Primitive evidence manifest has an unsupported schema")
    core = {key: value for key, value in manifest.items() if key != "manifest_id"}
    if manifest.get("manifest_id") != _identity(core):
        raise ValueError("Primitive evidence manifest identity is invalid")
    seen: set[str] = set()
    for entry in manifest.get("entries", []):
        if type(entry) is not dict or set(entry) != {
            "evidence_role",
            "path",
            "sha256",
            "source_url",
            "source_identifier",
            "original_retrieval_timestamp",
            "parser_schema_version",
        }:
            raise ValueError("Primitive evidence entry has the wrong schema")
        resolved = str(Path(entry["path"]).resolve(strict=True))
        if resolved in seen or _SHA256.fullmatch(str(entry["sha256"])) is None:
            raise ValueError("Primitive evidence paths or digests are invalid")
        seen.add(resolved)
        if sha256_file(resolved)[0] != entry["sha256"]:
            raise ValueError(f"Primitive evidence hash mismatch: {resolved}")
    return manifest


def _candidate_identities(entries: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    namespace = "{http://s3.amazonaws.com/doc/2006-03-01/}"
    prefixes: set[str] = set()
    paths = [entry["path"] for entry in entries if entry["evidence_role"] == "archive_candidate_index_xml"]
    if not paths:
        raise ValueError("Primitive manifest lacks archive candidate XML")
    for path in paths:
        root = ET.fromstring(Path(path).read_bytes())
        if root.tag != f"{namespace}ListBucketResult":
            raise ValueError("Archive candidate XML has the wrong namespace")
        for node in root.findall(f"{namespace}CommonPrefixes/{namespace}Prefix"):
            prefix = node.text
            base = "data/futures/um/monthly/klines/"
            if not prefix or not prefix.startswith(base) or not prefix.endswith("/"):
                raise ValueError("Archive candidate XML contains an invalid prefix")
            prefixes.add(prefix[len(base) : -1])
    canonical: list[str] = []
    quarantined: list[str] = []
    for identity in sorted(prefixes):
        try:
            canonical.append(require_binance_token(identity, "archive candidate"))
        except ValueError:
            quarantined.append(identity)
    return canonical, quarantined


def _replay_announcements(
    entries: list[dict[str, Any]], archive_symbols: set[str]
) -> tuple[pd.DataFrame, dict[str, Any]]:
    catalog_paths = [
        Path(entry["path"])
        for entry in entries
        if entry["evidence_role"] == "cms_catalog_response"
    ]
    detail_paths = {
        path.name.split("_")[1]: path
        for path in (Path(entry["path"]) for entry in entries)
        if path.name.startswith("article_") and not path.name.endswith(".provenance.json")
    }
    retrieved = {
        str(Path(entry["path"]).resolve()): entry["original_retrieval_timestamp"]
        for entry in entries
    }
    evidence: list[dict[str, Any]] = []
    audits: list[dict[str, Any]] = []
    detail_seen: set[tuple[str, str]] = set()
    for event_type, catalog_id in (
        ("listing", LISTING_CATALOG_ID),
        ("delisting", DELISTING_CATALOG_ID),
    ):
        matching = sorted(path for path in catalog_paths if f"catalog_{catalog_id}_page_" in path.name)
        articles: list[dict[str, Any]] = []
        declared: int | None = None
        for path in matching:
            response = _strict_json_bytes(path.read_bytes())
            catalogs = response.get("data", {}).get("catalogs")
            if response.get("code") != "000000" or type(catalogs) is not list or len(catalogs) != 1:
                raise ValueError("CMS catalog primitive is malformed")
            catalog = catalogs[0]
            if catalog.get("catalogId") != catalog_id or type(catalog.get("total")) is not int:
                raise ValueError("CMS catalog identity is invalid")
            if declared is None:
                declared = catalog["total"]
            elif declared != catalog["total"]:
                raise ValueError("CMS catalog totals changed across primitive pages")
            articles.extend(catalog.get("articles", []))
        if len(articles) != declared:
            raise ValueError("CMS catalog primitive pages are incomplete")
        candidates = [
            item
            for item in articles
            if type(item) is dict
            and type(item.get("title")) is str
            and (
                event_type == "delisting"
                or (
                    "binance futures" in item["title"].lower()
                    and ("launch" in item["title"].lower() or "adds" in item["title"].lower())
                    and ("perpetual" in item["title"].lower() or "contract" in item["title"].lower())
                    and "delivery" not in item["title"].lower()
                    and "quarterly" not in item["title"].lower()
                )
            )
        ]
        for article in candidates:
            code = article["code"]
            detail_key = (event_type, code)
            if detail_key in detail_seen:
                continue
            detail_seen.add(detail_key)
            detail_path = detail_paths.get(code)
            if detail_path is None:
                raise ValueError(f"CMS catalog article lacks a bound detail primitive: {code}")
            rows = parse_announcement_evidence(
                _strict_json_bytes(detail_path.read_bytes()),
                article,
                event_type,
                archive_symbols,
                retrieved_at=retrieved.get(str(detail_path.resolve())),
                raw_snapshot_path=str(detail_path.resolve()),
                raw_snapshot_sha256=sha256_file(detail_path)[0],
            )
            evidence.extend(asdict(row) for row in rows)
        audits.append(
            {
                "catalog_id": catalog_id,
                "event_type": event_type,
                "declared_total": declared,
                "pages": len(matching),
                "candidate_articles": len(candidates),
                "inspection_policy": (
                    "complete_catalog_detail_inspection"
                    if event_type == "delisting"
                    else "strict_positive_listing_title_prefilter"
                ),
            }
        )
    frame = pd.DataFrame(evidence)
    accepted = frame["match_status"].eq("accepted")
    duplicate = accepted & frame.loc[accepted].duplicated(
        ["event_type", "symbol"], keep=False
    ).reindex(frame.index, fill_value=False)
    frame.loc[duplicate, "match_status"] = "ambiguous_multiple_applicable_articles"
    return frame, {"parser_version": ANNOUNCEMENT_PARSER_VERSION, "catalogs": audits}


def _normalized_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return json.loads(json.dumps(frame.to_dict("records"), default=str, sort_keys=True))


def run_full_evidence_replay(
    report_root: str | Path,
    *,
    repository_root: str | Path,
    lifecycle_adjudications: dict[str, Any],
) -> dict[str, Any]:
    report = Path(report_root).resolve(strict=True)
    repository = Path(repository_root).resolve(strict=True)
    manifest_path = report / "primitive_evidence_manifest.json"
    manifest = verify_primitive_manifest(manifest_path)
    entries = manifest["entries"]
    canonical, quarantined = _candidate_identities(entries)
    candidates = sorted([*canonical, *quarantined])
    inventory = _strict_json_bytes((report / "candidate_inventory.json").read_bytes())
    if (
        inventory.get("candidate_identities") != candidates
        or inventory.get("candidate_set_digest") != candidate_set_digest(candidates)
    ):
        raise ValueError("Candidate inventory does not replay from primitive archive XML")
    scope_path = report / "historical_scope_registry.json"
    scope = _strict_json_bytes(scope_path.read_bytes())
    scope_by_symbol = verify_scope_registry(
        scope,
        candidates,
        registry_path=scope_path,
        repository_root=repository,
    )

    exchange_entries = [entry for entry in entries if entry["evidence_role"] == "exchange_info_snapshot"]
    if len(exchange_entries) != 1:
        raise ValueError("Primitive manifest requires exactly one exchangeInfo snapshot")
    exchange_entry = exchange_entries[0]
    exchange_payload = _strict_json_bytes(Path(exchange_entry["path"]).read_bytes())
    current_items = {item.get("symbol"): item for item in exchange_payload.get("symbols", [])}
    if len(current_items) != len(exchange_payload.get("symbols", [])):
        raise ValueError("exchangeInfo primitive contains duplicate symbols")
    for symbol in set(current_items) & set(scope_by_symbol):
        item = current_items[symbol]
        reviewed = scope_by_symbol[symbol]
        if (
            item.get("baseAsset") != reviewed.get("base_asset")
            or item.get("quoteAsset") != reviewed.get("quote_asset")
            or (item.get("underlyingType") == "COIN")
            is not reviewed.get("is_crypto_underlying")
        ):
            raise ValueError("Reviewed scope metadata disagrees with raw exchangeInfo")

    archive_rows = _strict_json_bytes((report / "archive_observations.json").read_bytes())
    manifest_paths = {entry["path"] for entry in entries}
    for row in archive_rows:
        paths = row.get("archive_raw_snapshot_paths")
        if type(paths) is not list or not paths or not set(paths).issubset(manifest_paths):
            raise ValueError("Archive observations reference unbound primitive XML")
        observed = list(observed_zip_keys_from_index_snapshots(paths, row["symbol"]))
        if observed != row.get("observed_archive_object_keys"):
            raise ValueError("Derived archive ZIP keys disagree with primitive XML replay")

    announcements, replay_audit = _replay_announcements(entries, set(canonical))
    stored_announcements = _strict_json_bytes((report / "announcement_evidence.json").read_bytes())
    if _normalized_records(announcements) != stored_announcements:
        raise ValueError("Derived announcement evidence disagrees with raw CMS replay")
    stored_audit = _strict_json_bytes((report / "announcement_corpus_audit.json").read_bytes())
    for replay_catalog, stored_catalog in zip(replay_audit["catalogs"], stored_audit["catalogs"]):
        for field in (
            "catalog_id",
            "event_type",
            "declared_total",
            "pages",
            "candidate_articles",
            "inspection_policy",
        ):
            if replay_catalog[field] != stored_catalog[field]:
                raise ValueError("Announcement corpus audit does not replay from primitives")

    first_trades = _strict_json_bytes((report / "first_observed_trades.json").read_bytes())
    bound_trade_paths = {
        str(Path(entry["path"]).resolve())
        for entry in entries
        if entry["evidence_role"] == "first_observed_trade_zip"
    }
    if {str(Path(row["raw_path"]).resolve()) for row in first_trades} != bound_trade_paths:
        raise ValueError("First-trade summaries do not exactly match bound primitive ZIPs")
    if first_trades:
        with ProcessPoolExecutor(max_workers=min(8, len(first_trades))) as executor:
            list(executor.map(verify_first_observed_trade_record, first_trades))
    boundaries = _strict_json_bytes(
        (report / "lifecycle_daily_trade_boundaries.json").read_bytes()
    )
    bound_boundary_paths = {
        entry["path"]
        for entry in entries
        if entry["evidence_role"] == "daily_trade_boundary_index_xml"
    }
    referenced_boundary_paths = {
        path for row in boundaries for path in row.get("raw_snapshot_paths", [])
    }
    if bound_boundary_paths != referenced_boundary_paths:
        raise ValueError("Daily trade boundary rows do not match bound primitive XML")
    verify_boundary_index(lifecycle_adjudications, boundaries)

    exchange_frame = records_from_exchange_info(
        exchange_payload,
        (
            pd.Timestamp(exchange_entry["original_retrieval_timestamp"])
            if exchange_entry["original_retrieval_timestamp"] is not None
            else None
        ),
        raw_snapshot_path=exchange_entry["path"],
        raw_snapshot_sha256=exchange_entry["sha256"],
    )
    stored_catalog = pd.DataFrame(
        _strict_json_bytes((report / "lifecycle_catalog.json").read_bytes())
    )
    replay_catalog = build_lifecycle_catalog(
        pd.DataFrame(archive_rows),
        exchange_frame,
        announcements,
        created_at=pd.Timestamp(stored_catalog.iloc[0]["catalog_created_at"]),
        announcement_search_completed=True,
        first_observed_trades=pd.DataFrame(first_trades),
        scope_registry_records=scope["records"],
        lifecycle_adjudications=lifecycle_adjudications,
    )
    if _normalized_records(replay_catalog) != _normalized_records(stored_catalog):
        raise ValueError("Lifecycle catalog does not reproduce under full primitive replay")
    readiness = _strict_json_bytes((report / "readiness.json").read_bytes())
    core = {
        "schema_version": FULL_REPLAY_SCHEMA_VERSION,
        "status": "PASS",
        "primitive_manifest_id": manifest["manifest_id"],
        "primitive_manifest_sha256": sha256_file(manifest_path)[0],
        "candidate_set_digest": inventory["candidate_set_digest"],
        "candidate_count": len(candidates),
        "reviewed_scope_registry_id": scope["registry_id"],
        "scope_independent_review_id": scope["independent_review"]["identifier"],
        "lifecycle_adjudication_id": lifecycle_adjudications["adjudication_id"],
        "archive_symbol_rows_replayed": len(archive_rows),
        "cms_evidence_rows_replayed": len(announcements),
        "first_trade_zips_rehashed_and_reparsed": len(first_trades),
        "daily_boundary_indexes_replayed": len(boundaries),
        "catalog_rows_reproduced": len(replay_catalog),
        "authorization_ready": readiness.get("authorization_ready"),
    }
    return {**core, "verification_report_id": _identity(core)}
