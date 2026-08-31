from __future__ import annotations

import hashlib
import json
import re
import xml.etree.ElementTree as ET
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict
from datetime import timedelta
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
    FUTURES_SERVER_TIME_URL,
    _parse_s3_index_request,
    _validate_s3_replay_page_chain,
    observed_daily_trade_keys_from_index_snapshots,
    observed_zip_keys_from_index_snapshots,
    parse_futures_server_time,
    sha256_file,
    validate_daily_trade_object_key,
    verify_first_observed_trade_record,
)
from alt_hot_scanner.data.provenance import (
    PROVENANCE_SCHEMA_VERSION,
    load_snapshot_provenance,
    provenance_path,
)
from alt_hot_scanner.identity import require_binance_token
from alt_hot_scanner.universe.adjudications import (
    episode_freshness_review_required,
    verify_boundary_index,
)
from alt_hot_scanner.universe.contracts import records_from_exchange_info
from alt_hot_scanner.universe.delisting_registry import (
    cms_corpus_binding,
    verify_delisting_records_against_announcement_evidence,
)
from alt_hot_scanner.universe.lifecycle import build_lifecycle_catalog
from alt_hot_scanner.universe.scope_registry import (
    candidate_set_digest,
    verify_scope_registry,
)

PRIMITIVE_MANIFEST_SCHEMA_VERSION = "lifecycle-primitive-evidence-manifest-v1"
FULL_REPLAY_SCHEMA_VERSION = "lifecycle-full-evidence-replay-v1"
_SHA256 = re.compile(r"[0-9a-f]{64}")
_PROVENANCE_BOUND_SNAPSHOT_ROLES = {
    "archive_candidate_index_xml",
    "frontier_daily_candidate_index_xml",
    "archive_symbol_month_index_xml",
    "archive_observation_index_xml",
    "daily_trade_boundary_index_xml",
    "daily_trade_frontier_index_xml",
    "exchange_info_snapshot",
    "cms_catalog_response",
    "cms_article_detail_response",
    "binance_server_time_snapshot",
}


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
    episode_trades: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    root = Path(raw_root).resolve(strict=True)
    trade_by_path = {
        str(Path(row["raw_path"]).resolve()): row
        for row in [*first_trades, *(episode_trades or [])]
    }
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
            elif relative.startswith("archive_index/frontier_daily_symbols_page_"):
                role = "frontier_daily_candidate_index_xml"
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
            elif relative.startswith("daily_trade_frontier_index/"):
                role = "daily_trade_frontier_index_xml"
            elif relative == "binance_futures_server_time.json":
                role = "binance_server_time_snapshot"
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
    raw_root_value = manifest.get("raw_root")
    if type(raw_root_value) is not str:
        raise ValueError("Primitive evidence manifest raw_root is invalid")
    raw_root = Path(raw_root_value).resolve(strict=True)
    if not raw_root.is_dir():
        raise ValueError("Primitive evidence manifest raw_root must be an existing directory")
    entries = manifest.get("entries")
    if type(entries) is not list:
        raise ValueError("Primitive evidence manifest entries must be a list")
    seen: set[str] = set()
    for entry in entries:
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
        if type(entry["path"]) is not str:
            raise ValueError("Primitive evidence path is invalid")
        resolved_path = Path(entry["path"]).resolve(strict=True)
        if not resolved_path.is_file() or raw_root not in resolved_path.parents:
            raise ValueError("Primitive evidence path resolves outside raw_root")
        resolved = str(resolved_path)
        if resolved in seen or _SHA256.fullmatch(str(entry["sha256"])) is None:
            raise ValueError("Primitive evidence paths or digests are invalid")
        seen.add(resolved)
        if sha256_file(resolved)[0] != entry["sha256"]:
            raise ValueError(f"Primitive evidence hash mismatch: {resolved}")
    sidecar_entries = {
        str(Path(entry["path"]).resolve()): entry
        for entry in entries
        if entry["evidence_role"] == "provenance_sidecar"
    }
    expected_sidecars = {
        str(sidecar.resolve())
        for sidecar in raw_root.rglob("*.provenance.json")
        if sidecar.is_file()
    }
    if expected_sidecars != set(sidecar_entries):
        raise ValueError("Primitive manifest does not close over raw provenance sidecars")
    for entry in entries:
        if entry["evidence_role"] not in _PROVENANCE_BOUND_SNAPSHOT_ROLES:
            continue
        snapshot = Path(entry["path"])
        sidecar = provenance_path(snapshot).resolve(strict=True)
        sidecar_entry = sidecar_entries.get(str(sidecar))
        if sidecar_entry is None:
            raise ValueError(f"Replay snapshot lacks a manifest-bound provenance sidecar: {snapshot}")
        payload = _strict_json_bytes(sidecar.read_bytes())
        if set(payload) != {
            "schema_version",
            "url",
            "sha256",
            "local_snapshot_path",
            "original_retrieval_timestamp",
            "acquisition_parser_version",
        } or payload.get("schema_version") != PROVENANCE_SCHEMA_VERSION:
            raise ValueError("Raw source provenance has the wrong canonical schema")
        if sidecar_entry["sha256"] != sha256_file(sidecar)[0]:
            raise ValueError(f"Primitive provenance sidecar hash mismatch: {sidecar}")
        if (
            entry["evidence_role"] == "binance_server_time_snapshot"
            and entry["source_url"] != FUTURES_SERVER_TIME_URL
        ):
            raise ValueError("Binance server-time primitive has the wrong source URL")
        bound = load_snapshot_provenance(
            snapshot,
            expected_url=entry["source_url"],
            expected_sha256=entry["sha256"],
        )
        if bound is None or bound.get("local_snapshot_path") != entry["path"]:
            raise ValueError("Primitive snapshot provenance is not manifest-bound")
        if (
            bound.get("sha256") != entry["sha256"]
            or bound.get("url") != entry["source_url"]
            or bound.get("original_retrieval_timestamp")
            != entry["original_retrieval_timestamp"]
            or bound.get("acquisition_parser_version")
            != entry["parser_schema_version"]
        ):
            raise ValueError("Primitive snapshot and provenance manifest bindings disagree")
    return manifest


def _candidate_identities(entries: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    layers, _ = _replay_discovery_layers(entries)
    identities = sorted(
        {
            identity
            for layer in layers
            for identity in layer["candidate_identities"]
        }
    )
    canonical: list[str] = []
    quarantined: list[str] = []
    for identity in identities:
        try:
            canonical.append(require_binance_token(identity, "archive candidate"))
        except ValueError:
            quarantined.append(identity)
    return canonical, quarantined


_DISCOVERY_LAYER_SPECS = (
    (
        "historical_monthly_candidates",
        "archive_candidate_index_xml",
        "data/futures/um/monthly/klines/",
    ),
    (
        "frontier_daily_candidates",
        "frontier_daily_candidate_index_xml",
        "data/futures/um/daily/klines/",
    ),
)


def _replay_discovery_layer(
    name: str,
    role: str,
    source_prefix: str,
    layer_entries: list[dict[str, Any]],
    *,
    expected_urls: dict[str, str | None],
) -> dict[str, Any]:
    if not layer_entries:
        raise ValueError(f"Primitive manifest lacks {name} evidence")
    paths = [entry["path"] for entry in layer_entries]
    prefixes, keys = _replay_s3_index_pages(
        paths,
        source_prefix,
        expected_urls=expected_urls,
    )
    namespace = "{http://s3.amazonaws.com/doc/2006-03-01/}"
    any_page_truncated = False
    for path in paths:
        root = ET.fromstring(Path(path).read_bytes())
        truncated_nodes = root.findall(f"{namespace}IsTruncated")
        if len(truncated_nodes) != 1:
            raise ValueError("Archive index has an invalid truncation marker")
        any_page_truncated = any_page_truncated or truncated_nodes[0].text == "true"

    unique_prefixes = sorted(set(prefixes))
    identities: list[str] = []
    quarantined: list[str] = []
    for prefix in unique_prefixes:
        if not prefix.startswith(source_prefix) or not prefix.endswith("/"):
            raise ValueError("Archive candidate XML contains an invalid returned prefix")
        identity = prefix[len(source_prefix) : -1]
        if "/" in identity:
            raise ValueError("Archive candidate XML contains an unexpectedly nested prefix")
        try:
            identities.append(require_binance_token(identity, "archive candidate"))
        except ValueError:
            quarantined.append(identity)

    source_urls = [entry["source_url"] for entry in layer_entries]
    index_audit = {
        "page_count": len(paths),
        "returned_prefix_count": len(prefixes),
        "returned_key_count": len(keys),
        "unique_prefix_count": len(unique_prefixes),
        "unique_key_count": len(set(keys)),
        "any_page_truncated": any_page_truncated,
        "source_urls": source_urls,
    }
    return {
        "layer": name,
        "source_prefix": source_prefix,
        "candidate_identities": sorted([*identities, *quarantined]),
        "raw_snapshot_paths": paths,
        "raw_snapshot_sha256s": [entry.get("sha256") for entry in layer_entries],
        "retrieval_provenance": "immutable_raw_snapshot_sidecars",
        "source_urls": source_urls,
        "index_audit": index_audit,
    }


def _replay_discovery_layers(
    entries: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[str]]:
    expected_urls = {
        str(Path(entry["path"]).resolve()): entry["source_url"]
        for entry in entries
    }
    layers = [
        _replay_discovery_layer(
            name,
            role,
            source_prefix,
            [entry for entry in entries if entry["evidence_role"] == role],
            expected_urls=expected_urls,
        )
        for name, role, source_prefix in _DISCOVERY_LAYER_SPECS
    ]
    candidates = sorted(
        {
            identity
            for layer in layers
            for identity in layer["candidate_identities"]
        }
    )
    return layers, candidates


def _replay_s3_index_pages(
    paths: list[str],
    expected_prefix: str,
    *,
    expected_urls: dict[str, str],
    expected_delimiter: str | None = "/",
) -> tuple[list[str], list[str]]:
    """Replay preserved ListObjectsV2 pages, including their pagination chain."""
    namespace = "{http://s3.amazonaws.com/doc/2006-03-01/}"
    page_numbers = []
    for path in paths:
        match = re.search(r"_page_(\d{3})_[0-9a-f]{16}\.xml$", Path(path).name)
        if match is None:
            raise ValueError("Archive index snapshot has no canonical page identity")
        page_numbers.append(int(match.group(1)))
    if page_numbers != list(range(1, len(page_numbers) + 1)):
        raise ValueError("Archive index snapshots have a missing or reordered page")
    prefixes: list[str] = []
    keys: list[str] = []
    states: list[tuple[bool, str | None, str | None]] = []
    for path in paths:
        source = Path(path)
        digest = sha256_file(source)[0]
        provenance = load_snapshot_provenance(
            source,
            expected_url=expected_urls.get(str(source.resolve())),
            expected_sha256=digest,
        )
        if provenance is None:
            raise ValueError("Archive index snapshot lacks immutable provenance")
        requested_token = _parse_s3_index_request(
            provenance["url"], expected_prefix, expected_delimiter
        )
        root = ET.fromstring(source.read_bytes())
        if root.tag != f"{namespace}ListBucketResult":
            raise ValueError("Archive candidate XML has the wrong namespace")
        truncated_nodes = root.findall(f"{namespace}IsTruncated")
        if len(truncated_nodes) != 1 or truncated_nodes[0].text not in {"true", "false"}:
            raise ValueError("Archive index has an invalid truncation marker")
        truncated = truncated_nodes[0].text == "true"
        page_prefixes = [
            node.text for node in root.findall(f"{namespace}CommonPrefixes/{namespace}Prefix")
        ]
        page_keys = [node.text for node in root.findall(f"{namespace}Contents/{namespace}Key")]
        if any(
            value is None or not value.startswith(expected_prefix)
            for value in [*page_prefixes, *page_keys]
        ) or any(value is not None and not value.endswith("/") for value in page_prefixes):
            raise ValueError("Archive candidate XML contains an invalid returned entry")
        key_counts = root.findall(f"{namespace}KeyCount")
        if len(key_counts) != 1 or key_counts[0].text != str(len(page_prefixes) + len(page_keys)):
            raise ValueError("Archive index KeyCount disagrees with returned entries")
        next_nodes = root.findall(f"{namespace}NextContinuationToken")
        if len(next_nodes) > 1:
            raise ValueError("Archive index has duplicate continuation tokens")
        next_token = next_nodes[0].text if next_nodes else None
        states.append((truncated, next_token, requested_token))
        prefixes.extend(value for value in page_prefixes if value is not None)
        keys.extend(value for value in page_keys if value is not None)
    _validate_s3_replay_page_chain(
        page_numbers,
        states,
        label="Archive index",
    )
    return prefixes, keys


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
        detail_checksums: list[dict[str, str]] = []
        for article in candidates:
            code = article["code"]
            detail_key = (event_type, code)
            if detail_key in detail_seen:
                continue
            detail_seen.add(detail_key)
            detail_path = detail_paths.get(code)
            if detail_path is None:
                raise ValueError(f"CMS catalog article lacks a bound detail primitive: {code}")
            detail_checksums.append({"article_code": code, "sha256": sha256_file(detail_path)[0]})
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
                "page_sha256s": [sha256_file(path)[0] for path in matching],
                "detail_sha256s": sorted(
                    detail_checksums, key=lambda item: item["article_code"]
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
    expected_provenance_urls = {
        str(Path(entry["path"]).resolve()): entry["source_url"]
        for entry in entries
        if entry["evidence_role"] in _PROVENANCE_BOUND_SNAPSHOT_ROLES
    }
    replayed_discovery_layers, candidates = _replay_discovery_layers(entries)
    replayed_candidate_set_digest = candidate_set_digest(candidates)
    inventory = _strict_json_bytes((report / "candidate_inventory.json").read_bytes())
    if (
        inventory.get("candidate_identities") != candidates
        or inventory.get("candidate_set_digest") != replayed_candidate_set_digest
    ):
        raise ValueError("Candidate inventory does not replay from primitive archive XML")
    layer_entries = {
        name: [entry for entry in entries if entry["evidence_role"] == role]
        for name, role, _source_prefix in _DISCOVERY_LAYER_SPECS
    }
    stored_layers = inventory.get("discovery_layers")
    if stored_layers != replayed_discovery_layers:
        raise ValueError("Candidate inventory discovery layers are incomplete")
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
    archive_symbols = {row.get("symbol") for row in archive_rows}
    expected_catalog_symbols = {
        symbol
        for symbol, reviewed in scope_by_symbol.items()
        if reviewed.get("product_scope") in {"in_scope_crypto_perpetual", "benchmark_only"}
    }
    if not expected_catalog_symbols.issubset(archive_symbols):
        raise ValueError("Lifecycle archive rows omit a reviewed in-scope candidate")
    for row in archive_rows:
        paths = row.get("archive_raw_snapshot_paths")
        frontier_only = row.get("archive_discovery_provenance") == (
            "frontier_daily_candidate_only_no_monthly_archive_blocking"
        )
        if frontier_only:
            if paths != [] or row.get("observed_archive_object_keys") != []:
                raise ValueError("Frontier-only archive rows cannot bind monthly ZIP evidence")
            continue
        if type(paths) is not list or not paths or not set(paths).issubset(manifest_paths):
            raise ValueError("Archive observations reference unbound primitive XML")
        observed = list(
            observed_zip_keys_from_index_snapshots(
                paths,
                row["symbol"],
                expected_provenance_urls=expected_provenance_urls,
            )
        )
        if observed != row.get("observed_archive_object_keys"):
            raise ValueError("Derived archive ZIP keys disagree with primitive XML replay")

    announcements, replay_audit = _replay_announcements(entries, set(candidates))
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
            "page_sha256s",
            "detail_sha256s",
        ):
            if replay_catalog[field] != stored_catalog[field]:
                raise ValueError("Announcement corpus audit does not replay from primitives")
    delisting_registry = _strict_json_bytes(
        (report / "historical_delisting_cutoff_registry.json").read_bytes()
    )
    replay_corpus = cms_corpus_binding(replay_audit)
    if delisting_registry.get("official_cms_corpus") != replay_corpus:
        raise ValueError("Reviewed delisting registry does not bind the replayed CMS corpus")
    verify_delisting_records_against_announcement_evidence(
        delisting_registry, announcements.to_dict("records")
    )

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
    episode_evidence = _strict_json_bytes(
        (report / "episode_first_observed_trades.json").read_bytes()
    )
    episode_core = {
        key: value for key, value in episode_evidence.items() if key != "evidence_id"
    }
    if (
        episode_evidence.get("evidence_id") != _identity(episode_core)
        or episode_evidence.get("candidate_set_digest") != inventory["candidate_set_digest"]
    ):
        raise ValueError("Episode first-trade evidence identity does not replay")
    episode_records = episode_evidence.get("records")
    if type(episode_records) is not list:
        raise ValueError("Episode first-trade evidence records are malformed")
    episode_paths = {
        entry["path"]
        for entry in entries
        if entry["evidence_role"] == "episode_first_observed_trade_zip"
    }
    if {str(Path(row["raw_path"]).resolve()) for row in episode_records} != (
        bound_trade_paths | episode_paths
    ):
        raise ValueError("Episode first-trade records do not exactly match bound primitives")
    for record in episode_records:
        verify_first_observed_trade_record(record)
    boundaries = _strict_json_bytes(
        (report / "lifecycle_daily_trade_boundaries.json").read_bytes()
    )
    bound_boundary_paths = {
        entry["path"]
        for entry in entries
        if entry["evidence_role"] in {
            "daily_trade_boundary_index_xml",
            "daily_trade_frontier_index_xml",
        }
    }
    referenced_boundary_paths = {
        path for row in boundaries for path in row.get("raw_snapshot_paths", [])
    }
    if bound_boundary_paths != referenced_boundary_paths:
        raise ValueError("Daily trade boundary rows do not match bound primitive XML")
    expected_boundary_symbols = {
        symbol
        for symbol, reviewed in scope_by_symbol.items()
        if reviewed.get("product_scope") in {"in_scope_crypto_perpetual", "benchmark_only"}
    }
    boundary_symbols = {row.get("symbol") for row in boundaries}
    if boundary_symbols != expected_boundary_symbols:
        raise ValueError("Daily trade boundary rows do not exactly cover reviewed candidates")
    if any(type(row.get("raw_snapshot_paths")) is not list or not row["raw_snapshot_paths"] for row in boundaries):
        raise ValueError("Daily trade boundary rows must bind a complete raw index chain")
    verify_boundary_index(lifecycle_adjudications, boundaries)

    freshness = _strict_json_bytes((report / "lifecycle_freshness.json").read_bytes())
    expected_frontier_candidate_discovery = {
        "layers": replayed_discovery_layers,
        "candidate_set_digest": replayed_candidate_set_digest,
    }
    if freshness.get("frontier_candidate_discovery") != expected_frontier_candidate_discovery:
        raise ValueError("Freshness frontier candidate discovery does not replay exactly")
    from alt_hot_scanner.universe.authorization import validate_lifecycle_freshness

    validate_lifecycle_freshness(freshness, report_root=report)
    cms_started = pd.Timestamp(stored_audit["rebuild_started_at"])
    if cms_started.tzinfo is None:
        cms_started = cms_started.tz_localize("UTC")
    else:
        cms_started = cms_started.tz_convert("UTC")
    if freshness["announcement_corpus"]["acquisition_started_at_utc"] != cms_started.isoformat().replace(
        "+00:00", "Z"
    ):
        raise ValueError("Freshness CMS acquisition-start evidence does not replay")
    replayed_boundaries = []
    for row in boundaries:
        paths = row.get("raw_snapshot_paths")
        try:
            replayed_keys = list(
                observed_daily_trade_keys_from_index_snapshots(
                    paths,
                    row["symbol"],
                    expected_provenance_urls=expected_provenance_urls,
                )
            )
        except ValueError as exc:
            if "No daily trade ZIP objects" not in str(exc):
                raise
            replayed_keys = []
        replayed_dates = sorted(
            {validate_daily_trade_object_key(key).period for key in replayed_keys}
        )
        for field in ("observed_archive_keys", "observed_archive_dates", "observed_daily_trade_dates"):
            expected = replayed_keys if field == "observed_archive_keys" else replayed_dates
            if row.get(field) != expected:
                raise ValueError("Derived daily trade boundaries disagree with raw index replay")
        replayed_boundaries.append(row)
    freshness_rows = freshness["episode_freshness_evidence"].get("symbols")
    if freshness_rows != replayed_boundaries:
        raise ValueError("Episode freshness evidence does not reproduce daily trade indexes")
    if episode_freshness_review_required(
        boundaries,
        lifecycle_adjudications,
        required_valid_through_utc=freshness["required_valid_through_utc"],
        reviewed_delisting_records=delisting_registry["records"],
    ) is not None:
        raise ValueError("Daily trade frontier evidence requires lifecycle review")
    server_entries = [
        entry for entry in entries if entry["evidence_role"] == "binance_server_time_snapshot"
    ]
    if len(server_entries) != 1:
        raise ValueError("Primitive manifest requires one Binance server-time snapshot")
    server_entry = server_entries[0]
    if freshness["binance_server_time_evidence"].get("path") != server_entry["path"] or freshness[
        "binance_server_time_evidence"
    ].get("sha256") != server_entry["sha256"]:
        raise ValueError("Freshness server-time evidence is not bound to its primitive")
    server_time = parse_futures_server_time(Path(server_entry["path"]).read_bytes())
    expected_server_horizon = (
        server_time.replace(hour=0, minute=0, second=0, microsecond=0)
        - timedelta(days=1)
    ).isoformat().replace("+00:00", "Z")
    if freshness["binance_server_time_evidence"].get(
        "maximum_archive_backed_horizon_utc"
    ) != expected_server_horizon:
        raise ValueError("Freshness server-time horizon does not replay")
    replay_corpus = cms_corpus_binding(replay_audit)
    if freshness["announcement_corpus"] != {
        "identity": replay_corpus["identity"],
        "sha256": replay_corpus["sha256"],
        "acquisition_started_at_utc": freshness["announcement_corpus"][
            "acquisition_started_at_utc"
        ],
    }:
        raise ValueError("Freshness announcement corpus binding does not replay")
    if freshness["candidate_valid_through_utc"] != expected_server_horizon:
        raise ValueError("Candidate freshness horizon does not replay from server time")
    if freshness["episode_valid_through_utc"] != expected_server_horizon:
        raise ValueError("Episode freshness horizon does not replay from server time")
    expected_delisting_horizon = min(
        expected_server_horizon,
        freshness["announcement_corpus"]["acquisition_started_at_utc"],
        key=lambda value: pd.Timestamp(value),
    )
    if freshness["delisting_valid_through_utc"] != expected_delisting_horizon:
        raise ValueError("Delisting freshness horizon does not replay from server and CMS evidence")

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
        episode_first_observed_trades=episode_records,
        delisting_registry_records=delisting_registry["records"],
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
        "frontier_candidate_index_pages_replayed": len(layer_entries["frontier_daily_candidates"]),
        "episode_freshness_evidence_id": _identity(freshness["episode_freshness_evidence"]),
        "lifecycle_freshness_id": freshness["freshness_id"],
        "lifecycle_evidence_valid_through_utc": freshness[
            "lifecycle_evidence_valid_through_utc"
        ],
        "catalog_rows_reproduced": len(replay_catalog),
        "authorization_ready": readiness.get("authorization_ready"),
    }
    return {**core, "verification_report_id": _identity(core)}
