from __future__ import annotations

"""Fail-closed acquisition authorization and mixed-source planning for Binance USD-M 1H data.

This module is deliberately acquisition-owned.  The lifecycle verifier continues to use the
frozen monthly-only parser in :mod:`alt_hot_scanner.data.binance_public`.
"""

import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import unicodedata
import urllib.error
import urllib.parse
import xml.etree.ElementTree as ET
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from typing import Any

import pandas as pd

from alt_hot_scanner.data.binance_public import (
    ARCHIVE_HOST,
    INDEX_HOST,
    _read_url,
    parse_checksum_sidecar,
    sha256_file,
    write_bytes_exclusive,
    write_json_exclusive,
)
from alt_hot_scanner.data.normalize import validate_normalized_1h
from alt_hot_scanner.identity import require_archive_symbol_identity, require_canonical_text

SOURCE_POLICY_VERSION = "monthly-daily-api-v2"
ACQUISITION_AUTH_SCHEMA_VERSION = "acquisition-executable-authorization-v1"
PLAN_SCHEMA_VERSION = "full-history-mixed-source-plan-v1"
RAW_COMPLETION_SCHEMA_VERSION = "raw-completion-manifest-v1"
ATTEMPT_SCHEMA_VERSION = "plan-bound-attempt-manifest-v2"

LIFECYCLE_CRITICAL_PATHS = (
    "src/alt_hot_scanner/data/binance_public.py",
    "src/alt_hot_scanner/identity.py",
    "src/alt_hot_scanner/universe/authorization.py",
    "src/alt_hot_scanner/universe/contracts.py",
    "src/alt_hot_scanner/universe/eligibility.py",
    "src/alt_hot_scanner/universe/eligibility_oracle.py",
    "src/alt_hot_scanner/universe/lifecycle.py",
    "src/alt_hot_scanner/universe/scope_registry.py",
    "src/alt_hot_scanner/utils/config.py",
    "src/alt_hot_scanner/utils/numeric.py",
)

ACQUISITION_EXECUTABLE_PATHS = (
    "pyproject.toml",
    "config/research_v0_1.yaml",
    "scripts/download_from_plan.py",
    "scripts/prepare_full_manifest.py",
    "scripts/process_downloaded_archives.py",
    "src/alt_hot_scanner/analysis/splits.py",
    "src/alt_hot_scanner/data/acquisition.py",
    "src/alt_hot_scanner/data/aggregate.py",
    "src/alt_hot_scanner/data/full_history.py",
    "src/alt_hot_scanner/data/normalize.py",
    "src/alt_hot_scanner/features/core.py",
    "src/alt_hot_scanner/outcomes/labels.py",
    "src/alt_hot_scanner/scanner/episodes.py",
    "src/alt_hot_scanner/scanner/scoring.py",
)

_DAILY_KLINE_KEY = re.compile(
    r"data/futures/um/daily/klines/"
    r"(?P<symbol>[^/\\]{1,128})/1h/"
    r"(?P=symbol)-1h-(?P<period>20[0-9]{2}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01]))\.zip"
)


class AcquisitionInvariantError(ValueError):
    """A fail-closed acquisition/build invariant was violated."""


@dataclass(frozen=True)
class AcquisitionArchiveIdentity:
    object_key: str
    symbol: str
    interval: str
    period: str
    filename: str
    source_kind: str


@dataclass(frozen=True)
class SourceEntry:
    source_kind: str
    symbol: str
    object_key_or_request: str
    source_period: str
    discovery_evidence: tuple[str, ...]
    selected_canonical_range: tuple[str, str]


@dataclass(frozen=True)
class FrozenRunIdentity:
    cutoff_exclusive_utc: str
    lifecycle_bundle_id: str
    lifecycle_approval_id: str
    lifecycle_evidence_code_commit: str
    acquisition_executable_commit: str
    acquisition_executable_tree_sha256: str
    acquisition_authorization_id: str
    config_sha256: str
    source_inventory_sha256: str
    source_policy_version: str
    split_definitions: dict[str, Any]
    run_id: str


@dataclass(frozen=True)
class GapRecord:
    kind: str
    symbol: str
    start: str | None
    end: str | None
    evidence: str
    resolution: str | None = None


@dataclass(frozen=True)
class DownloadedSource:
    source_kind: str
    symbol: str
    object_key_or_request: str
    status: str
    raw_path: str
    raw_sha256: str
    byte_count: int
    retrieved_at: str
    published_sha256: str | None = None
    checksum_sidecar_path: str | None = None
    checksum_sidecar_sha256: str | None = None
    payload_source: str | None = None
    upstream_revision_detected: bool = False


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def digest_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def sha256_path(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def floor_to_1h(value: pd.Timestamp | datetime) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC").floor("h")


def freeze_cutoff_from_server(server_time_ms: int) -> str:
    if type(server_time_ms) is not int or server_time_ms < 0:
        raise ValueError("Binance server time must be a non-negative integer milliseconds value")
    return floor_to_1h(pd.to_datetime(server_time_ms, unit="ms", utc=True)).isoformat()


def fetch_frozen_cutoff(
    server_reader: Callable[[], bytes] = lambda: _read_url("https://fapi.binance.com/fapi/v1/time"),
) -> tuple[str, bytes]:
    raw = server_reader()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise AcquisitionInvariantError("Binance server time response is invalid JSON") from exc
    if type(payload) is not dict or set(payload) != {"serverTime"} or type(payload["serverTime"]) is not int:
        raise AcquisitionInvariantError("Binance server time response is not exact")
    return freeze_cutoff_from_server(payload["serverTime"]), raw


def _paths_digest(root: Path, paths: Iterable[str]) -> str:
    records: list[dict[str, str]] = []
    for relative in sorted(set(paths)):
        path = (root / relative).resolve()
        if not path.is_relative_to(root.resolve()) or not path.is_file():
            raise AcquisitionInvariantError(f"Executable path is missing or escapes repository: {relative}")
        records.append({"path": relative, "sha256": sha256_path(path)})
    return digest_json(records)


def acquisition_executable_tree_sha256(root: str | Path) -> str:
    return _paths_digest(Path(root).resolve(), ACQUISITION_EXECUTABLE_PATHS)


def _paths_digest_at_commit(root: Path, commit: str, paths: Iterable[str]) -> str:
    records: list[dict[str, str]] = []
    for relative in sorted(set(paths)):
        try:
            payload = subprocess.run(
                ["git", "show", f"{commit}:{relative}"],
                cwd=root,
                check=True,
                capture_output=True,
            ).stdout
        except subprocess.CalledProcessError as exc:
            raise AcquisitionInvariantError(
                f"Reviewed acquisition commit lacks executable path: {relative}"
            ) from exc
        records.append({"path": relative, "sha256": hashlib.sha256(payload).hexdigest()})
    return digest_json(records)


def verify_lifecycle_runtime_boundary(root: str | Path, lifecycle_commit: str) -> None:
    """Prove the transitive lifecycle verifier boundary still matches the frozen commit."""
    repository = Path(root).resolve()
    result = subprocess.run(
        ["git", "diff", "--quiet", lifecycle_commit, "--", *LIFECYCLE_CRITICAL_PATHS],
        cwd=repository,
        check=False,
    )
    if result.returncode != 0:
        raise AcquisitionInvariantError("Lifecycle-critical runtime differs from frozen lifecycle executable")
    untracked = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all", "--", *LIFECYCLE_CRITICAL_PATHS],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if any(line.startswith("?? ") for line in untracked.splitlines()):
        raise AcquisitionInvariantError("Lifecycle-critical runtime contains untracked code")


def verify_acquisition_authorization(path: str | Path, root: str | Path) -> dict[str, Any]:
    authorization_path = Path(path).resolve(strict=True)
    repository = Path(root).resolve()
    authorization = json.loads(authorization_path.read_text(encoding="utf-8"))
    required = {
        "schema_version",
        "purpose",
        "acquisition_executable_commit",
        "acquisition_paths",
        "acquisition_tree_sha256",
        "review_artifact",
        "status",
        "authorized_at",
        "authorization_id",
    }
    if type(authorization) is not dict or set(authorization) != required:
        raise AcquisitionInvariantError("Acquisition authorization fields do not match exact schema")
    if authorization["schema_version"] != ACQUISITION_AUTH_SCHEMA_VERSION:
        raise AcquisitionInvariantError("Unsupported acquisition authorization schema")
    core = {key: value for key, value in authorization.items() if key != "authorization_id"}
    if authorization["authorization_id"] != digest_json(core):
        raise AcquisitionInvariantError("Acquisition authorization identity is invalid")
    if authorization["purpose"] != "full_history_acquisition_and_performance_blind_build":
        raise AcquisitionInvariantError("Acquisition authorization has wrong purpose")
    if authorization["status"] != "active":
        raise AcquisitionInvariantError("Acquisition authorization is not active")
    if authorization["acquisition_paths"] != list(ACQUISITION_EXECUTABLE_PATHS):
        raise AcquisitionInvariantError("Acquisition authorization path boundary changed")
    commit = authorization["acquisition_executable_commit"]
    try:
        subprocess.run(["git", "cat-file", "-e", f"{commit}^{{commit}}"], cwd=repository, check=True, capture_output=True)
    except subprocess.CalledProcessError as exc:
        raise AcquisitionInvariantError("Reviewed acquisition executable commit is unavailable") from exc
    commit_tree = _paths_digest_at_commit(repository, commit, ACQUISITION_EXECUTABLE_PATHS)
    if authorization["acquisition_tree_sha256"] != commit_tree:
        raise AcquisitionInvariantError("Acquisition authorization tree does not match reviewed commit")
    actual_tree = acquisition_executable_tree_sha256(repository)
    if authorization["acquisition_tree_sha256"] != actual_tree:
        raise AcquisitionInvariantError("Runtime acquisition executable differs from reviewed authorization")
    review = authorization["review_artifact"]
    if type(review) is not dict or set(review) != {"path", "sha256", "verdict"} or review["verdict"] != "PASS":
        raise AcquisitionInvariantError("Acquisition executable lacks an exact PASS review artifact")
    review_path = Path(review["path"])
    if not review_path.is_absolute() or not review_path.is_file() or sha256_path(review_path) != review["sha256"]:
        raise AcquisitionInvariantError("Acquisition review artifact digest is invalid")
    return authorization


def create_run_identity(
    *,
    cutoff_exclusive_utc: str,
    lifecycle_bundle_id: str,
    lifecycle_approval_id: str,
    lifecycle_evidence_code_commit: str,
    acquisition_executable_commit: str,
    acquisition_executable_tree_sha256: str,
    acquisition_authorization_id: str,
    config_sha256: str,
    source_inventory_sha256: str,
    split_definitions: dict[str, Any],
    run_id: str,
) -> FrozenRunIdentity:
    cutoff = floor_to_1h(pd.Timestamp(cutoff_exclusive_utc)).isoformat()
    return FrozenRunIdentity(
        cutoff,
        lifecycle_bundle_id,
        lifecycle_approval_id,
        lifecycle_evidence_code_commit,
        acquisition_executable_commit,
        acquisition_executable_tree_sha256,
        acquisition_authorization_id,
        config_sha256,
        source_inventory_sha256,
        SOURCE_POLICY_VERSION,
        split_definitions,
        run_id,
    )


def validate_daily_kline_object_key(object_key: object) -> AcquisitionArchiveIdentity:
    if type(object_key) is not str or not object_key or object_key != object_key.strip():
        raise ValueError("daily object_key must be nonempty canonical text")
    if "\\" in object_key or unicodedata.normalize("NFC", object_key) != object_key:
        raise ValueError("daily object_key must use canonical Unicode and POSIX separators")
    match = _DAILY_KLINE_KEY.fullmatch(object_key)
    if match is None:
        raise ValueError("object_key is outside Binance USD-M daily 1H kline hierarchy")
    try:
        datetime.strptime(match.group("period"), "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError as exc:
        raise ValueError("Daily object-key period is not a real UTC date") from exc
    symbol = require_archive_symbol_identity(match.group("symbol"), "daily object_key symbol")
    return AcquisitionArchiveIdentity(
        object_key,
        symbol,
        "1h",
        match.group("period"),
        object_key.rsplit("/", 1)[-1],
        "daily",
    )


def validate_acquisition_archive_key(object_key: object) -> AcquisitionArchiveIdentity:
    from alt_hot_scanner.data.binance_public import validate_archive_object_key

    try:
        monthly = validate_archive_object_key(object_key)
    except ValueError:
        return validate_daily_kline_object_key(object_key)
    return AcquisitionArchiveIdentity(
        monthly.object_key,
        monthly.symbol,
        monthly.interval,
        monthly.period,
        monthly.filename,
        "monthly",
    )


def _parse_index_page(payload: bytes, prefix: str) -> tuple[bool, str | None, list[str], list[str]]:
    namespace_uri = "http://s3.amazonaws.com/doc/2006-03-01/"
    namespace = {"s3": namespace_uri}
    try:
        root = ET.fromstring(payload)
    except ET.ParseError as exc:
        raise AcquisitionInvariantError("Malformed archive-index XML") from exc
    if root.tag != f"{{{namespace_uri}}}ListBucketResult":
        raise AcquisitionInvariantError("Archive-index XML has unexpected root/namespace")
    truncated_nodes = root.findall("s3:IsTruncated", namespace)
    if len(truncated_nodes) != 1 or truncated_nodes[0].text not in {"true", "false"}:
        raise AcquisitionInvariantError("Archive-index page requires one canonical IsTruncated")
    page_prefixes = [node.text for node in root.findall("s3:CommonPrefixes/s3:Prefix", namespace)]
    page_keys = [node.text for node in root.findall("s3:Contents/s3:Key", namespace)]
    if any(not value or not value.startswith(prefix) for value in [*page_prefixes, *page_keys]):
        raise AcquisitionInvariantError("Archive-index page returned an invalid/out-of-prefix entry")
    key_count_nodes = root.findall("s3:KeyCount", namespace)
    if key_count_nodes:
        if len(key_count_nodes) != 1:
            raise AcquisitionInvariantError("Archive-index page contains duplicate KeyCount")
        try:
            count = int(key_count_nodes[0].text or "")
        except ValueError as exc:
            raise AcquisitionInvariantError("Archive-index KeyCount is malformed") from exc
        if count != len(page_prefixes) + len(page_keys):
            raise AcquisitionInvariantError("Archive-index KeyCount disagrees with entries")
    token_nodes = root.findall("s3:NextContinuationToken", namespace)
    if len(token_nodes) > 1:
        raise AcquisitionInvariantError("Archive-index page contains duplicate continuation tokens")
    token = token_nodes[0].text if token_nodes else None
    return truncated_nodes[0].text == "true", token, [x for x in page_prefixes if x], [x for x in page_keys if x]


def list_daily_archive_index(
    prefix: str,
    *,
    delimiter: str | None = None,
    reader: Callable[[str], bytes] = _read_url,
    page_observer: Callable[[int, str, bytes], None] | None = None,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[dict[str, Any], ...]]:
    """Fully paginate only the acquisition-owned Binance daily kline hierarchy."""
    root_prefix = "data/futures/um/daily/klines/"
    if type(prefix) is not str or not prefix.startswith(root_prefix) or "\\" in prefix:
        raise ValueError("Daily archive prefix is outside approved Binance USD-M hierarchy")
    if unicodedata.normalize("NFC", prefix) != prefix or delimiter not in {None, "/"}:
        raise ValueError("Daily archive prefix/delimiter is not canonical")
    token: str | None = None
    seen_tokens: set[str] = set()
    prefixes: list[str] = []
    keys: list[str] = []
    evidence: list[dict[str, Any]] = []
    page = 0
    while True:
        params = {"list-type": "2", "prefix": prefix}
        if delimiter is not None:
            params["delimiter"] = delimiter
        if token is not None:
            params["continuation-token"] = token
        url = f"{INDEX_HOST}?{urllib.parse.urlencode(params)}"
        payload = reader(url)
        page += 1
        if page_observer is not None:
            page_observer(page, url, payload)
        truncated, next_token, page_prefixes, page_keys = _parse_index_page(payload, prefix)
        prefixes.extend(page_prefixes)
        keys.extend(page_keys)
        evidence.append({"page": page, "url": url, "sha256": hashlib.sha256(payload).hexdigest()})
        if not truncated:
            if next_token is not None and next_token.strip():
                raise AcquisitionInvariantError("Untruncated archive-index page supplied continuation token")
            break
        if next_token is None or not next_token.strip():
            raise AcquisitionInvariantError("Truncated archive-index page lacks continuation token")
        next_token = require_canonical_text(next_token, "daily archive continuation token")
        if next_token in seen_tokens:
            raise AcquisitionInvariantError("Daily archive pagination repeated continuation token")
        seen_tokens.add(next_token)
        token = next_token
    return tuple(sorted(set(prefixes))), tuple(sorted(set(keys))), tuple(evidence)


def discover_daily_1h_objects(
    *,
    reader: Callable[[str], bytes] = _read_url,
    page_observer: Callable[[int, str, bytes], None] | None = None,
) -> tuple[list[AcquisitionArchiveIdentity], list[dict[str, Any]]]:
    """Discover exact observed daily 1H objects, including symbol prefixes on every page."""
    root = "data/futures/um/daily/klines/"
    symbol_prefixes, _, root_evidence = list_daily_archive_index(
        root, delimiter="/", reader=reader, page_observer=page_observer
    )
    identities: dict[str, AcquisitionArchiveIdentity] = {}
    evidence = list(root_evidence)
    for symbol_prefix in symbol_prefixes:
        symbol = symbol_prefix.removeprefix(root).rstrip("/")
        try:
            symbol = require_archive_symbol_identity(symbol, "daily discovery symbol")
        except ValueError:
            continue
        prefix = f"{root}{symbol}/1h/"
        _, keys, pages = list_daily_archive_index(
            prefix, reader=reader, page_observer=page_observer
        )
        evidence.extend(pages)
        key_set = set(keys)
        for key in keys:
            candidate = key.removesuffix(".CHECKSUM")
            try:
                identity = validate_daily_kline_object_key(candidate)
            except ValueError as exc:
                raise AcquisitionInvariantError(f"Unexpected object in daily 1H listing: {key}") from exc
            if identity.symbol != symbol:
                raise AcquisitionInvariantError("Daily 1H listing returned mismatched symbol")
            if not key.endswith(".CHECKSUM"):
                if f"{key}.CHECKSUM" not in key_set:
                    raise AcquisitionInvariantError("Observed daily ZIP lacks observed checksum sidecar")
                identities[identity.object_key] = identity
    return sorted(identities.values(), key=lambda x: (x.symbol, x.period, x.object_key)), evidence


def replay_daily_discovery_pages(
    page_records: list[dict[str, Any]],
) -> tuple[list[AcquisitionArchiveIdentity], list[dict[str, Any]]]:
    """Re-derive exact daily objects and pagination completeness from frozen raw XML pages."""
    if type(page_records) is not list or not page_records:
        raise AcquisitionInvariantError("Daily discovery evidence is empty")
    required = {"sequence", "page", "url", "sha256", "path"}
    groups: dict[tuple[str, str | None], list[dict[str, Any]]] = {}
    parsed_pages: dict[int, tuple[bool, str | None, list[str], list[str]]] = {}
    seen_sequences: set[int] = set()
    for record in page_records:
        if type(record) is not dict or set(record) != required:
            raise AcquisitionInvariantError("Daily discovery page descriptor schema is invalid")
        sequence = record["sequence"]
        if type(sequence) is not int or sequence <= 0 or sequence in seen_sequences:
            raise AcquisitionInvariantError("Daily discovery page sequence is invalid")
        seen_sequences.add(sequence)
        path = Path(record["path"]).resolve(strict=True)
        payload = path.read_bytes()
        if hashlib.sha256(payload).hexdigest() != record["sha256"]:
            raise AcquisitionInvariantError("Daily discovery page bytes changed")
        parsed_url = urllib.parse.urlsplit(record["url"])
        index_base = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
        if index_base != INDEX_HOST:
            raise AcquisitionInvariantError("Daily discovery page uses wrong archive index host")
        query = urllib.parse.parse_qs(parsed_url.query, strict_parsing=True)
        if query.get("list-type") != ["2"] or len(query.get("prefix", [])) != 1:
            raise AcquisitionInvariantError("Daily discovery page URL is not canonical ListObjectsV2")
        prefix = query["prefix"][0]
        delimiter_values = query.get("delimiter")
        delimiter = delimiter_values[0] if delimiter_values else None
        if delimiter not in {None, "/"}:
            raise AcquisitionInvariantError("Daily discovery evidence uses invalid delimiter")
        parsed_pages[sequence] = _parse_index_page(payload, prefix)
        groups.setdefault((prefix, delimiter), []).append(record)

    if seen_sequences != set(range(1, len(page_records) + 1)):
        raise AcquisitionInvariantError("Daily discovery evidence sequence is incomplete")

    group_outputs: dict[tuple[str, str | None], tuple[list[str], list[str]]] = {}
    for group_key, records in groups.items():
        ordered = sorted(records, key=lambda item: item["page"])
        if [item["page"] for item in ordered] != list(range(1, len(ordered) + 1)):
            raise AcquisitionInvariantError("Daily discovery pagination page numbers are incomplete")
        prefixes: list[str] = []
        keys: list[str] = []
        expected_token: str | None = None
        for position, record in enumerate(ordered):
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(record["url"]).query, strict_parsing=True)
            actual_token_values = query.get("continuation-token")
            actual_token = actual_token_values[0] if actual_token_values else None
            if actual_token != expected_token:
                raise AcquisitionInvariantError("Daily discovery continuation-token chain is broken")
            truncated, next_token, page_prefixes, page_keys = parsed_pages[record["sequence"]]
            prefixes.extend(page_prefixes)
            keys.extend(page_keys)
            if truncated:
                if next_token is None or not next_token.strip():
                    raise AcquisitionInvariantError("Truncated frozen discovery page lacks next token")
                expected_token = require_canonical_text(next_token, "frozen daily continuation token")
                if position == len(ordered) - 1:
                    raise AcquisitionInvariantError("Frozen daily discovery stopped on a truncated page")
            else:
                if next_token is not None and next_token.strip():
                    raise AcquisitionInvariantError("Untruncated frozen page has a next token")
                expected_token = None
                if position != len(ordered) - 1:
                    raise AcquisitionInvariantError("Frozen daily discovery has pages after terminal page")
        group_outputs[group_key] = (prefixes, keys)

    root = "data/futures/um/daily/klines/"
    root_group = group_outputs.get((root, "/"))
    if root_group is None:
        raise AcquisitionInvariantError("Frozen daily discovery lacks root symbol-prefix listing")
    root_prefixes, root_keys = root_group
    if root_keys:
        raise AcquisitionInvariantError("Daily root discovery unexpectedly contains direct objects")
    expected_symbol_groups: set[tuple[str, str | None]] = set()
    identities: dict[str, AcquisitionArchiveIdentity] = {}
    for symbol_prefix in sorted(set(root_prefixes)):
        symbol_text = symbol_prefix.removeprefix(root).rstrip("/")
        try:
            symbol = require_archive_symbol_identity(symbol_text, "frozen daily discovery symbol")
        except ValueError:
            continue
        key = (f"{root}{symbol}/1h/", None)
        expected_symbol_groups.add(key)
        output = group_outputs.get(key)
        if output is None:
            raise AcquisitionInvariantError(f"Frozen daily discovery omitted 1H listing for {symbol}")
        nested_prefixes, keys = output
        if nested_prefixes:
            raise AcquisitionInvariantError("Daily symbol/1H discovery unexpectedly returned prefixes")
        key_set = set(keys)
        for object_key in keys:
            candidate = object_key.removesuffix(".CHECKSUM")
            try:
                identity = validate_daily_kline_object_key(candidate)
            except ValueError as exc:
                raise AcquisitionInvariantError(f"Unexpected frozen daily 1H object: {object_key}") from exc
            if identity.symbol != symbol:
                raise AcquisitionInvariantError("Frozen daily object symbol mismatches prefix")
            if not object_key.endswith(".CHECKSUM"):
                if f"{object_key}.CHECKSUM" not in key_set:
                    raise AcquisitionInvariantError("Observed daily ZIP lacks observed checksum sidecar")
                identities[identity.object_key] = identity
    orphan_groups = set(group_outputs) - {(root, "/")} - expected_symbol_groups
    if orphan_groups:
        raise AcquisitionInvariantError("Frozen daily discovery contains orphan/unrequested listing groups")
    evidence = [
        {"sha256": record["sha256"], "url": record["url"]}
        for record in sorted(page_records, key=lambda item: item["sequence"])
    ]
    return sorted(identities.values(), key=lambda item: (item.symbol, item.period, item.object_key)), evidence


def source_period_bounds(source_kind: str, period: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    if source_kind == "monthly":
        start = pd.Timestamp(f"{period}-01", tz="UTC")
        return start, start + pd.offsets.MonthBegin(1)
    if source_kind == "daily":
        start = pd.Timestamp(period, tz="UTC")
        return start, start + pd.Timedelta(days=1)
    raise ValueError("Archive source kind must be monthly or daily")


def request_url(request: dict[str, Any]) -> str:
    parameters = {key: value for key, value in request.items() if key != "endpoint"}
    return f"{request['endpoint']}?{urllib.parse.urlencode(parameters)}"


def api_tail_requests(symbol: str, start: pd.Timestamp, cutoff_exclusive_utc: str) -> list[dict[str, Any]]:
    start = floor_to_1h(start)
    cutoff = floor_to_1h(pd.Timestamp(cutoff_exclusive_utc))
    requests: list[dict[str, Any]] = []
    cursor = start
    max_hours = 1500
    while cursor < cutoff:
        page_end = min(cursor + pd.Timedelta(hours=max_hours), cutoff)
        requests.append(
            {
                "endpoint": "https://fapi.binance.com/fapi/v1/klines",
                "symbol": symbol,
                "interval": "1h",
                "startTime": int(cursor.timestamp() * 1000),
                "endTime": int(page_end.timestamp() * 1000) - 1,
                "limit": max_hours,
            }
        )
        cursor = page_end
    return requests


def validate_api_tail(rows: list[list[Any]], request: dict[str, Any], cutoff_exclusive_utc: str) -> None:
    start = pd.to_datetime(request["startTime"], unit="ms", utc=True)
    request_end = pd.to_datetime(request["endTime"] + 1, unit="ms", utc=True)
    cutoff = floor_to_1h(pd.Timestamp(cutoff_exclusive_utc))
    if request_end > cutoff or request_end <= start:
        raise AcquisitionInvariantError("API request is outside the frozen latest suffix")
    expected = start
    seen: set[int] = set()
    for row in rows:
        if type(row) is not list or len(row) < 7:
            raise AcquisitionInvariantError("Malformed API kline row")
        opened_ms = int(row[0])
        opened = pd.to_datetime(opened_ms, unit="ms", utc=True)
        if opened != expected or opened >= request_end or opened >= cutoff or opened_ms in seen:
            raise AcquisitionInvariantError("API is permitted only as an exact contiguous suffix")
        seen.add(opened_ms)
        expected += pd.Timedelta(hours=1)
    if expected != request_end:
        raise AcquisitionInvariantError("API response is short/empty before its frozen page boundary")


def _source_entry_for_archive(identity: AcquisitionArchiveIdentity, evidence: Iterable[str]) -> SourceEntry:
    start, end = source_period_bounds(identity.source_kind, identity.period)
    return SourceEntry(
        identity.source_kind,
        identity.symbol,
        identity.object_key,
        identity.period,
        tuple(evidence),
        (start.isoformat(), end.isoformat()),
    )


def _source_entry_for_api(request: dict[str, Any]) -> SourceEntry:
    start = pd.to_datetime(request["startTime"], unit="ms", utc=True)
    end = pd.to_datetime(request["endTime"] + 1, unit="ms", utc=True)
    return SourceEntry(
        "api",
        request["symbol"],
        request_url(request),
        f"{request['startTime']}-{request['endTime']}",
        ("Binance USD-M /fapi/v1/klines",),
        (start.isoformat(), end.isoformat()),
    )


def source_inventory(entries: list[SourceEntry]) -> dict[str, Any]:
    rows = [
        asdict(entry)
        for entry in sorted(
            entries,
            key=lambda x: (x.symbol, x.selected_canonical_range[0], x.source_kind, x.object_key_or_request),
        )
    ]
    return {
        "schema_version": "source-inventory-v2",
        "source_policy_version": SOURCE_POLICY_VERSION,
        "entries": rows,
        "inventory_sha256": digest_json(rows),
    }


def verify_source_policy(entries: list[SourceEntry], cutoff_exclusive_utc: str) -> None:
    """Enforce monthly > daily > API, zero selected overlap, and exact API suffix ending at cutoff."""
    cutoff = floor_to_1h(pd.Timestamp(cutoff_exclusive_utc))
    by_symbol: dict[str, list[SourceEntry]] = {}
    for entry in entries:
        if entry.source_kind not in {"monthly", "daily", "api"}:
            raise AcquisitionInvariantError("Unsupported canonical source kind")
        start = pd.Timestamp(entry.selected_canonical_range[0])
        end = pd.Timestamp(entry.selected_canonical_range[1])
        if start >= end or end > cutoff:
            raise AcquisitionInvariantError("Selected source range exceeds frozen cutoff")
        by_symbol.setdefault(entry.symbol, []).append(entry)
    for symbol, rows in by_symbol.items():
        ordered = sorted(rows, key=lambda row: pd.Timestamp(row.selected_canonical_range[0]))
        previous_end: pd.Timestamp | None = None
        seen_api = False
        for row in ordered:
            start = pd.Timestamp(row.selected_canonical_range[0])
            end = pd.Timestamp(row.selected_canonical_range[1])
            if previous_end is not None and start < previous_end:
                raise AcquisitionInvariantError(f"Canonical sources overlap for {symbol}")
            if seen_api and row.source_kind != "api":
                raise AcquisitionInvariantError("Archive source appears after API suffix")
            if row.source_kind == "api":
                seen_api = True
            previous_end = end
        api_rows = [row for row in ordered if row.source_kind == "api"]
        if api_rows:
            if pd.Timestamp(api_rows[-1].selected_canonical_range[1]) != cutoff:
                raise AcquisitionInvariantError("API suffix must end exactly at frozen cutoff")
            for left, right in pairwise(api_rows):
                if pd.Timestamp(left.selected_canonical_range[1]) != pd.Timestamp(
                    right.selected_canonical_range[0]
                ):
                    raise AcquisitionInvariantError("API pages are not a contiguous latest suffix")


def _lifecycle_source_active_between(
    lifecycle_catalog: pd.DataFrame, symbol: str, start: pd.Timestamp, end: pd.Timestamp
) -> bool:
    rows = lifecycle_catalog.loc[lifecycle_catalog["symbol"].eq(symbol)]
    if len(rows) != 1:
        raise AcquisitionInvariantError(f"Lifecycle catalog identity missing/duplicated: {symbol}")
    row = rows.iloc[0]
    intervals = row.get("lifecycle_intervals")
    if isinstance(intervals, str) and intervals:
        intervals = json.loads(intervals)
    if intervals:
        for interval in intervals:
            episode_start = pd.Timestamp(interval["age_live_anchor_at"]).floor("h")
            last = interval.get("last_trading_at")
            episode_end = (
                pd.Timestamp(last).floor("h") + pd.Timedelta(hours=1) if last else end
            )
            if episode_start < end and episode_end > start:
                return True
        return False
    anchor = row.get("eligibility_age_anchor_at") or row.get("first_observed_trade_at")
    if pd.isna(anchor):
        return False
    episode_start = pd.Timestamp(anchor).floor("h")
    last = row.get("last_trading_at")
    episode_end = pd.Timestamp(last).floor("h") + pd.Timedelta(hours=1) if pd.notna(last) else end
    return episode_start < end and episode_end > start


def build_mixed_source_inventory(
    *,
    monthly_keys: list[str],
    daily_identities: list[AcquisitionArchiveIdentity],
    approved_symbols: set[str],
    frozen_candidate_universe: set[str],
    warmup_start_month: str,
    cutoff_exclusive_utc: str,
    daily_discovery_evidence: Iterable[dict[str, Any]],
    lifecycle_catalog: pd.DataFrame,
) -> tuple[dict[str, Any], str]:
    """Select global monthly->daily boundary and only a trailing API suffix.

    Any missing daily day followed by a later observed day is an interior archive gap and fails.
    A missing trailing daily suffix is represented only by API requests, never by repair of history.
    """
    cutoff = floor_to_1h(pd.Timestamp(cutoff_exclusive_utc))
    boundary = pd.Timestamp(year=cutoff.year, month=cutoff.month, day=1, tz="UTC")
    warmup_month = pd.Period(warmup_start_month, freq="M")
    unknown = sorted(
        {
            identity.symbol
            for identity in daily_identities
            if identity.symbol.endswith("USDT") and identity.symbol not in frozen_candidate_universe
        }
    )
    if unknown:
        raise AcquisitionInvariantError(
            f"Daily discovery exposed identities absent frozen lifecycle candidate universe: {unknown}"
        )
    evidence_tokens = tuple(sorted({row["sha256"] for row in daily_discovery_evidence}))
    entries: list[SourceEntry] = []
    monthly_by_symbol: dict[str, list[AcquisitionArchiveIdentity]] = {}
    for key in monthly_keys:
        identity = validate_acquisition_archive_key(key)
        if identity.source_kind != "monthly" or identity.symbol not in approved_symbols:
            continue
        month = pd.Period(identity.period, freq="M")
        _, end = source_period_bounds("monthly", identity.period)
        if warmup_month <= month and end <= boundary:
            monthly_by_symbol.setdefault(identity.symbol, []).append(identity)

    # Exact observed objects are necessary but not sufficient: a whole monthly object can be
    # absent from the observed set.  Reconcile the observed monthly tier against the frozen
    # lifecycle-active history inside the required acquisition window before any daily/API tail
    # is considered.  A missing historical month is an archive gap, never an API repair request.
    final_month = boundary.tz_localize(None).to_period("M") - 1
    required_months = pd.period_range(warmup_month, final_month, freq="M")
    for symbol in sorted(approved_symbols):
        observed_periods = {identity.period for identity in monthly_by_symbol.get(symbol, [])}
        for month in required_months:
            start = month.start_time.tz_localize("UTC")
            end = (month + 1).start_time.tz_localize("UTC")
            if (
                _lifecycle_source_active_between(lifecycle_catalog, symbol, start, end)
                and str(month) not in observed_periods
            ):
                raise AcquisitionInvariantError(
                    f"Missing lifecycle-active monthly archive for {symbol} at {month}; "
                    "API repair prohibited"
                )

    daily_by_symbol: dict[str, dict[pd.Timestamp, AcquisitionArchiveIdentity]] = {}
    for identity in daily_identities:
        if identity.symbol not in approved_symbols:
            continue
        start, end = source_period_bounds("daily", identity.period)
        if start >= boundary and end <= cutoff:
            daily_by_symbol.setdefault(identity.symbol, {})[start] = identity
    final_complete_day_end = cutoff.floor("D")
    for symbol in sorted(approved_symbols):
        for identity in sorted(monthly_by_symbol.get(symbol, []), key=lambda x: x.period):
            entries.append(_source_entry_for_archive(identity, ("frozen lifecycle monthly observation",)))
        days = daily_by_symbol.get(symbol, {})
        cursor = boundary
        latest_observed = max(days) if days else None
        while cursor < final_complete_day_end:
            if cursor in days:
                entries.append(_source_entry_for_archive(days[cursor], evidence_tokens))
                cursor += pd.Timedelta(days=1)
                continue
            if latest_observed is not None and latest_observed > cursor:
                raise AcquisitionInvariantError(
                    f"Interior daily archive gap for {symbol} at {cursor.date()}; API repair prohibited"
                )
            break
        if cursor < cutoff and _lifecycle_source_active_between(
            lifecycle_catalog, symbol, cursor, cutoff
        ):
            for request in api_tail_requests(symbol, cursor, cutoff.isoformat()):
                entries.append(_source_entry_for_api(request))
    verify_source_policy(entries, cutoff.isoformat())
    inventory = source_inventory(entries)
    return inventory, boundary.isoformat()


def create_frozen_plan(
    *,
    run_identity: FrozenRunIdentity,
    lifecycle_bundle: str,
    lifecycle_approval: str,
    acquisition_authorization: str,
    source_inventory_payload: dict[str, Any],
    monthly_daily_boundary_utc: str,
    server_time_evidence_path: str,
    server_time_evidence_sha256: str,
    daily_discovery_pages: list[dict[str, Any]],
) -> dict[str, Any]:
    if source_inventory_payload.get("inventory_sha256") != run_identity.source_inventory_sha256:
        raise AcquisitionInvariantError("Run identity source inventory digest mismatch")
    core = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "purpose": "authorized_full_history_mixed_source_acquisition",
        "run_identity": asdict(run_identity),
        "lifecycle_bundle": str(Path(lifecycle_bundle).resolve()),
        "lifecycle_approval": str(Path(lifecycle_approval).resolve()),
        "acquisition_authorization": str(Path(acquisition_authorization).resolve()),
        "source_inventory": source_inventory_payload,
        "monthly_daily_boundary_utc": monthly_daily_boundary_utc,
        "server_time_evidence": {
            "path": str(Path(server_time_evidence_path).resolve()),
            "sha256": server_time_evidence_sha256,
        },
        "daily_discovery_pages": daily_discovery_pages,
    }
    return {**core, "plan_integrity": digest_json(core)}


def verify_frozen_plan(plan_path: str | Path, root: str | Path) -> dict[str, Any]:
    from alt_hot_scanner.universe.authorization import verify_approval_pin, verify_lifecycle_bundle

    path = Path(plan_path).resolve(strict=True)
    repository = Path(root).resolve()
    plan = json.loads(path.read_text(encoding="utf-8"))
    required = {
        "schema_version",
        "purpose",
        "run_identity",
        "lifecycle_bundle",
        "lifecycle_approval",
        "acquisition_authorization",
        "source_inventory",
        "monthly_daily_boundary_utc",
        "server_time_evidence",
        "daily_discovery_pages",
        "plan_integrity",
    }
    if type(plan) is not dict or set(plan) != required or plan["schema_version"] != PLAN_SCHEMA_VERSION:
        raise AcquisitionInvariantError("Frozen mixed-source plan schema is invalid")
    core = {key: value for key, value in plan.items() if key != "plan_integrity"}
    if plan["plan_integrity"] != digest_json(core):
        raise AcquisitionInvariantError("Frozen mixed-source plan integrity is invalid")
    bundle_path = Path(plan["lifecycle_bundle"])
    approval_path = Path(plan["lifecycle_approval"])
    auth_path = Path(plan["acquisition_authorization"])
    if not all(item.is_absolute() for item in (bundle_path, approval_path, auth_path)):
        raise AcquisitionInvariantError("Plan authorization paths must be absolute")
    verified_bundle = verify_lifecycle_bundle(bundle_path)
    verified_approval = verify_approval_pin(approval_path, verified_bundle)
    lifecycle_commit = verified_approval["approval"]["lifecycle_evidence_code_commit"]
    verify_lifecycle_runtime_boundary(repository, lifecycle_commit)
    acquisition_auth = verify_acquisition_authorization(auth_path, repository)
    run = plan["run_identity"]
    expected_run_keys = {field.name for field in FrozenRunIdentity.__dataclass_fields__.values()}
    if set(run) != expected_run_keys:
        raise AcquisitionInvariantError("Run identity fields are not exact")
    if run["lifecycle_bundle_id"] != verified_bundle["bundle"]["bundle_id"]:
        raise AcquisitionInvariantError("Run identity lifecycle bundle mismatch")
    if run["lifecycle_approval_id"] != verified_approval["approval"]["approval_id"]:
        raise AcquisitionInvariantError("Run identity lifecycle approval mismatch")
    if run["lifecycle_evidence_code_commit"] != lifecycle_commit:
        raise AcquisitionInvariantError("Run identity lifecycle executable mismatch")
    if run["acquisition_executable_commit"] != acquisition_auth["acquisition_executable_commit"]:
        raise AcquisitionInvariantError("Run identity acquisition commit mismatch")
    if run["acquisition_authorization_id"] != acquisition_auth["authorization_id"]:
        raise AcquisitionInvariantError("Run identity acquisition authorization mismatch")
    if run["acquisition_executable_tree_sha256"] != acquisition_auth["acquisition_tree_sha256"]:
        raise AcquisitionInvariantError("Run identity acquisition executable mismatch")
    if run["config_sha256"] != verified_bundle["bundle"]["config"]["sha256"]:
        raise AcquisitionInvariantError("Run identity config mismatch")
    inventory = plan["source_inventory"]
    if inventory.get("schema_version") != "source-inventory-v2":
        raise AcquisitionInvariantError("Plan source inventory schema is invalid")
    if inventory.get("inventory_sha256") != digest_json(inventory.get("entries")):
        raise AcquisitionInvariantError("Plan source inventory digest is invalid")
    if run["source_inventory_sha256"] != inventory["inventory_sha256"]:
        raise AcquisitionInvariantError("Run identity source inventory mismatch")
    if run["source_policy_version"] != SOURCE_POLICY_VERSION:
        raise AcquisitionInvariantError("Run identity source policy mismatch")
    server = plan["server_time_evidence"]
    server_path = Path(server.get("path", ""))
    if not server_path.is_absolute() or not server_path.is_file() or sha256_path(server_path) != server.get("sha256"):
        raise AcquisitionInvariantError("Frozen server-time evidence is missing/changed")
    cutoff, raw = fetch_frozen_cutoff(lambda: server_path.read_bytes())
    if hashlib.sha256(raw).hexdigest() != server["sha256"] or cutoff != run["cutoff_exclusive_utc"]:
        raise AcquisitionInvariantError("Frozen server-time evidence does not derive run cutoff")
    # Replay frozen discovery bytes and independently reconstruct the exact canonical source plan.
    replayed_daily, discovery_evidence = replay_daily_discovery_pages(plan["daily_discovery_pages"])
    from alt_hot_scanner.universe.contracts import filter_instrument_scope
    from alt_hot_scanner.utils.config import load_config

    config = load_config(verified_bundle["config_path"])
    catalog = verified_bundle["catalog"].copy()
    if "underlying_subtype" in catalog.columns:
        catalog["underlying_subtype"] = catalog["underlying_subtype"].map(
            lambda value: tuple(json.loads(value)) if isinstance(value, str) else value
        )
    scoped = filter_instrument_scope(catalog, config["universe"]["stablecoin_underlyings"])
    approved_symbols = set(
        scoped.loc[scoped["historical_inclusion_readiness"].eq("ready"), "symbol"]
    )
    candidate_inventory = json.loads(
        verified_bundle["artifacts"]["candidate_inventory.json"].read_text(encoding="utf-8")
    )
    frozen_candidates = set(candidate_inventory.get("candidate_identities", []))
    archive_rows = json.loads(
        verified_bundle["artifacts"]["archive_observations.json"].read_text(encoding="utf-8")
    )
    monthly_keys: list[str] = []
    for row in archive_rows:
        if row.get("symbol") in approved_symbols:
            observed = row.get("observed_archive_object_keys")
            if type(observed) is not list:
                raise AcquisitionInvariantError("Lifecycle archive observations lost exact object keys")
            monthly_keys.extend(observed)
    warmup_start = pd.Timestamp(config["data"]["start"]).tz_localize(None).to_period("M") - 1
    expected_inventory, expected_boundary = build_mixed_source_inventory(
        monthly_keys=sorted(set(monthly_keys)),
        daily_identities=replayed_daily,
        approved_symbols=approved_symbols,
        frozen_candidate_universe=frozen_candidates,
        warmup_start_month=str(warmup_start),
        cutoff_exclusive_utc=run["cutoff_exclusive_utc"],
        daily_discovery_evidence=discovery_evidence,
        lifecycle_catalog=verified_bundle["catalog"],
    )
    if expected_inventory != inventory or expected_boundary != plan["monthly_daily_boundary_utc"]:
        raise AcquisitionInvariantError("Frozen source inventory is not exact replay of authoritative observations")
    entries = [SourceEntry(**row) for row in inventory["entries"]]
    verify_source_policy(entries, run["cutoff_exclusive_utc"])
    return {
        "plan": plan,
        "plan_path": path,
        "verified_bundle": verified_bundle,
        "verified_approval": verified_approval,
        "acquisition_authorization": acquisition_auth,
        "approved_symbols": approved_symbols,
        "source_entries": entries,
    }


def _safe_versioned_archive_path(raw_root: Path, identity: AcquisitionArchiveIdentity, checksum: str) -> Path:
    root = raw_root.resolve()
    target = root / "archive_versions" / checksum / Path(identity.object_key)
    resolved = target.resolve(strict=False)
    if not resolved.is_relative_to(root):
        raise AcquisitionInvariantError("Archive destination escapes raw root")
    return resolved


def _parse_checksum_for_acquisition(payload: bytes, identity: AcquisitionArchiveIdentity) -> str:
    # Reuse the frozen parser by a shape-compatible identity; it checks exact filename/hash format.
    from alt_hot_scanner.data.binance_public import ArchiveObjectIdentity

    return parse_checksum_sidecar(
        payload,
        ArchiveObjectIdentity(identity.object_key, identity.symbol, identity.interval, identity.period, identity.filename),
    )


def acquire_archive_source(
    entry: SourceEntry,
    raw_root: str | Path,
    *,
    reader: Callable[[str], bytes] = _read_url,
    retrieved_at: str | None = None,
    run_id: str | None = None,
) -> DownloadedSource:
    identity = validate_acquisition_archive_key(entry.object_key_or_request)
    if entry.source_kind != identity.source_kind or entry.symbol != identity.symbol:
        raise AcquisitionInvariantError("Archive source entry identity mismatch")
    url = f"{ARCHIVE_HOST}/{identity.object_key}"
    checksum_url = f"{url}.CHECKSUM"
    checksum_bytes = reader(checksum_url)
    published = _parse_checksum_for_acquisition(checksum_bytes, identity)
    raw_root_path = Path(raw_root).resolve()
    sidecar_path = raw_root_path / "checksum_evidence" / published / f"{identity.filename}.CHECKSUM"
    sidecar_path.parent.mkdir(parents=True, exist_ok=True)
    if sidecar_path.exists():
        if sidecar_path.read_bytes() != checksum_bytes:
            raise AcquisitionInvariantError("Checksum evidence path is immutable")
    else:
        write_bytes_exclusive(sidecar_path, checksum_bytes)
    if run_id is not None:
        binding_key = hashlib.sha256(identity.object_key.encode("utf-8")).hexdigest()
        binding_path = raw_root_path / "run_checksum_bindings" / run_id / f"{binding_key}.json"
        binding = {
            "object_key": identity.object_key,
            "published_sha256": published,
            "checksum_sidecar_sha256": hashlib.sha256(checksum_bytes).hexdigest(),
        }
        if binding_path.exists():
            frozen = json.loads(binding_path.read_text(encoding="utf-8"))
            if frozen != binding:
                raise AcquisitionInvariantError(
                    "Binance upstream archive revision detected after this run froze checksum evidence"
                )
        else:
            binding_path.parent.mkdir(parents=True, exist_ok=True)
            write_json_exclusive(binding_path, binding)
    target = _safe_versioned_archive_path(raw_root_path, identity, published)
    target.parent.mkdir(parents=True, exist_ok=True)
    upstream_revision = False
    # Detect whether another immutable version of this exact object key was previously acquired.
    versions_root = raw_root_path / "archive_versions"
    if versions_root.exists():
        for candidate in versions_root.glob(f"*/{identity.object_key}"):
            if candidate.exists() and candidate.resolve() != target.resolve():
                upstream_revision = True
                break
    if target.exists():
        computed, byte_count = sha256_file(target)
        if computed != published:
            raise AcquisitionInvariantError("Frozen archive version is locally corrupted")
        payload_source = "existing_content_addressed_version_reverified"
    else:
        payload = reader(url)
        computed = hashlib.sha256(payload).hexdigest()
        byte_count = len(payload)
        if computed != published:
            raise AcquisitionInvariantError("Downloaded archive differs from published checksum")
        descriptor, temporary_name = tempfile.mkstemp(dir=target.parent, prefix=f".{target.name}.", suffix=".part")
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.link(temporary, target)
        except FileExistsError:
            existing, byte_count = sha256_file(target)
            if existing != published:
                raise AcquisitionInvariantError("Concurrent archive publication installed different bytes")
        finally:
            temporary.unlink(missing_ok=True)
        payload_source = "downloaded_verified_content_addressed_no_replace"
    return DownloadedSource(
        entry.source_kind,
        entry.symbol,
        entry.object_key_or_request,
        "verified",
        str(target),
        published,
        byte_count,
        retrieved_at or datetime.now(UTC).isoformat(),
        published,
        str(sidecar_path),
        hashlib.sha256(checksum_bytes).hexdigest(),
        payload_source,
        upstream_revision,
    )


def acquire_api_source(
    entry: SourceEntry,
    raw_root: str | Path,
    cutoff_exclusive_utc: str,
    *,
    reader: Callable[[str], bytes] = _read_url,
    retrieved_at: str | None = None,
    run_id: str | None = None,
) -> DownloadedSource:
    if entry.source_kind != "api":
        raise AcquisitionInvariantError("API acquisition received non-API source")
    parsed = urllib.parse.urlsplit(entry.object_key_or_request)
    query = urllib.parse.parse_qs(parsed.query, strict_parsing=True)
    request = {
        "endpoint": f"{parsed.scheme}://{parsed.netloc}{parsed.path}",
        "symbol": query["symbol"][0],
        "interval": query["interval"][0],
        "startTime": int(query["startTime"][0]),
        "endTime": int(query["endTime"][0]),
        "limit": int(query["limit"][0]),
    }
    if request_url(request) != entry.object_key_or_request or request["symbol"] != entry.symbol:
        raise AcquisitionInvariantError("API source request identity is not canonical")
    raw = reader(entry.object_key_or_request)
    try:
        rows = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise AcquisitionInvariantError("API response is invalid JSON") from exc
    if type(rows) is not list:
        raise AcquisitionInvariantError("API response must be a JSON array")
    validate_api_tail(rows, request, cutoff_exclusive_utc)
    digest = hashlib.sha256(raw).hexdigest()
    raw_root_path = Path(raw_root).resolve()
    if run_id is not None:
        request_digest = hashlib.sha256(entry.object_key_or_request.encode("utf-8")).hexdigest()
        binding_path = raw_root_path / "run_api_bindings" / run_id / f"{request_digest}.json"
        binding = {
            "request_url": entry.object_key_or_request,
            "raw_sha256": digest,
        }
        if binding_path.exists():
            frozen = json.loads(binding_path.read_text(encoding="utf-8"))
            if frozen != binding:
                raise AcquisitionInvariantError(
                    "Binance API response changed after this run froze exact response bytes"
                )
        else:
            binding_path.parent.mkdir(parents=True, exist_ok=True)
            write_json_exclusive(binding_path, binding)
    target = raw_root_path / "api_tail" / entry.symbol / f"{digest}.response"
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if target.read_bytes() != raw:
            raise AcquisitionInvariantError("API raw response content-addressed path changed")
    else:
        write_bytes_exclusive(target, raw)
    metadata = {
        "request_url": entry.object_key_or_request,
        "retrieved_at": retrieved_at or datetime.now(UTC).isoformat(),
        "raw_sha256": digest,
        "byte_count": len(raw),
    }
    metadata_path = target.with_suffix(".metadata.json")
    if not metadata_path.exists():
        write_json_exclusive(metadata_path, metadata)
    return DownloadedSource(
        "api",
        entry.symbol,
        entry.object_key_or_request,
        "verified",
        str(target),
        digest,
        len(raw),
        metadata["retrieved_at"],
        payload_source="exact_raw_api_response_bytes",
    )


def require_complete_attempt_manifest(
    verified_plan: dict[str, Any], manifest: dict[str, Any], *, canonical: bool = True
) -> list[dict[str, Any]]:
    if not canonical or manifest.get("canonical") is not True:
        raise AcquisitionInvariantError("Smoke/limited attempt manifest is non-canonical")
    if manifest.get("schema_version") != ATTEMPT_SCHEMA_VERSION:
        raise AcquisitionInvariantError("Attempt manifest schema is invalid")
    plan = verified_plan["plan"]
    if manifest.get("plan_path") != str(verified_plan["plan_path"]):
        raise AcquisitionInvariantError("Attempt manifest does not point to authoritative plan")
    if manifest.get("plan_integrity") != plan["plan_integrity"]:
        raise AcquisitionInvariantError("Attempt manifest plan integrity mismatch")
    if manifest.get("run_id") != plan["run_identity"]["run_id"]:
        raise AcquisitionInvariantError("Attempt manifest run identity mismatch")
    expected = {entry.object_key_or_request for entry in verified_plan["source_entries"]}
    attempts = manifest.get("attempts")
    if type(attempts) is not list or any(type(row) is not dict for row in attempts):
        raise AcquisitionInvariantError("Attempt manifest attempts are malformed")
    actual = [row.get("object_key_or_request") for row in attempts]
    if len(actual) != len(set(actual)) or set(actual) != expected:
        raise AcquisitionInvariantError("Attempt set is not the exact authoritative source plan")
    if any(row.get("status") != "verified" for row in attempts):
        raise AcquisitionInvariantError("Missing/failed source attempts block canonical completion")
    return attempts


def build_raw_completion_manifest(
    verified_plan: dict[str, Any], attempt_manifest_path: str | Path, raw_root: str | Path
) -> dict[str, Any]:
    attempt_path = Path(attempt_manifest_path).resolve(strict=True)
    manifest = json.loads(attempt_path.read_text(encoding="utf-8"))
    attempts = require_complete_attempt_manifest(verified_plan, manifest, canonical=True)
    records: list[dict[str, Any]] = []
    for attempt in sorted(attempts, key=lambda row: row["object_key_or_request"]):
        raw_path = Path(attempt["raw_path"]).resolve(strict=True)
        root = Path(raw_root).resolve()
        if not raw_path.is_relative_to(root):
            raise AcquisitionInvariantError("Attempt raw path escapes raw root")
        actual, byte_count = sha256_file(raw_path)
        if actual != attempt.get("raw_sha256") or byte_count != attempt.get("byte_count"):
            raise AcquisitionInvariantError("Attempt raw bytes changed before completion")
        if attempt.get("source_kind") in {"monthly", "daily"}:
            sidecar = Path(attempt.get("checksum_sidecar_path", "")).resolve(strict=True)
            if not sidecar.is_relative_to(root) or sha256_path(sidecar) != attempt.get("checksum_sidecar_sha256"):
                raise AcquisitionInvariantError("Archive checksum evidence changed before completion")
        records.append(attempt)
    core = {
        "schema_version": RAW_COMPLETION_SCHEMA_VERSION,
        "run_id": verified_plan["plan"]["run_identity"]["run_id"],
        "plan_path": str(verified_plan["plan_path"]),
        "plan_integrity": verified_plan["plan"]["plan_integrity"],
        "attempt_manifest_path": str(attempt_path),
        "attempt_manifest_sha256": sha256_path(attempt_path),
        "source_records": records,
    }
    return {**core, "completion_id": digest_json(core)}


def verify_raw_completion(
    completion_path: str | Path, verified_plan: dict[str, Any], raw_root: str | Path
) -> dict[str, Any]:
    path = Path(completion_path).resolve(strict=True)
    completion = json.loads(path.read_text(encoding="utf-8"))
    required = {
        "schema_version",
        "run_id",
        "plan_path",
        "plan_integrity",
        "attempt_manifest_path",
        "attempt_manifest_sha256",
        "source_records",
        "completion_id",
    }
    if type(completion) is not dict or set(completion) != required:
        raise AcquisitionInvariantError("Raw completion fields are not exact")
    core = {key: value for key, value in completion.items() if key != "completion_id"}
    if completion["schema_version"] != RAW_COMPLETION_SCHEMA_VERSION or completion["completion_id"] != digest_json(core):
        raise AcquisitionInvariantError("Raw completion identity is invalid")
    if completion["run_id"] != verified_plan["plan"]["run_identity"]["run_id"]:
        raise AcquisitionInvariantError("Raw completion run ID mismatch")
    if completion["plan_path"] != str(verified_plan["plan_path"]) or completion["plan_integrity"] != verified_plan["plan"]["plan_integrity"]:
        raise AcquisitionInvariantError("Raw completion is not bound to authoritative plan")
    attempt_path = Path(completion["attempt_manifest_path"]).resolve(strict=True)
    if sha256_path(attempt_path) != completion["attempt_manifest_sha256"]:
        raise AcquisitionInvariantError("Attempt manifest changed after raw completion")
    attempt_manifest = json.loads(attempt_path.read_text(encoding="utf-8"))
    attempts = require_complete_attempt_manifest(verified_plan, attempt_manifest, canonical=True)
    if completion["source_records"] != sorted(attempts, key=lambda row: row["object_key_or_request"]):
        raise AcquisitionInvariantError("Raw completion source records differ from authoritative attempts")
    root = Path(raw_root).resolve()
    for record in completion["source_records"]:
        raw_path = Path(record["raw_path"]).resolve(strict=True)
        if not raw_path.is_relative_to(root):
            raise AcquisitionInvariantError("Raw completion path escapes raw root")
        actual, byte_count = sha256_file(raw_path)
        if actual != record["raw_sha256"] or byte_count != record["byte_count"]:
            raise AcquisitionInvariantError("Raw source changed after Gate A input was frozen")
        if record["source_kind"] in {"monthly", "daily"}:
            sidecar = Path(record["checksum_sidecar_path"]).resolve(strict=True)
            if not sidecar.is_relative_to(root) or sha256_path(sidecar) != record["checksum_sidecar_sha256"]:
                raise AcquisitionInvariantError("Archive checksum evidence changed")
            identity = validate_acquisition_archive_key(record["object_key_or_request"])
            published = _parse_checksum_for_acquisition(sidecar.read_bytes(), identity)
            if published != record["published_sha256"] or published != record["raw_sha256"]:
                raise AcquisitionInvariantError("Frozen checksum evidence no longer matches raw archive")
    return completion


def validate_source_containment(frame: pd.DataFrame, source_kind: str, period: str) -> None:
    start, end = source_period_bounds(source_kind, period)
    if frame.empty:
        return
    if frame["open_time"].min() < start or frame["open_time"].max() >= end:
        raise AcquisitionInvariantError("Rows spill outside source period encoded by object key")


def classify_gaps(
    symbol: str,
    timestamps: pd.Series,
    expected_start: str | None = None,
    expected_end: str | None = None,
) -> list[GapRecord]:
    values = pd.Series(pd.to_datetime(timestamps, utc=True)).sort_values().drop_duplicates().reset_index(drop=True)
    result: list[GapRecord] = []
    start = pd.Timestamp(expected_start) if expected_start else None
    end = pd.Timestamp(expected_end) if expected_end else None
    if values.empty:
        if start is not None and end is not None and start < end:
            result.append(
                GapRecord("unexplained_missing_planned_archive", symbol, start.isoformat(), end.isoformat(), "empty canonical series")
            )
        return result
    if start is not None and values.iloc[0] > start:
        result.append(GapRecord("leading_source_gap", symbol, start.isoformat(), values.iloc[0].isoformat(), "canonical series starts late"))
    for previous, current in zip(values.iloc[:-1], values.iloc[1:]):
        if current - previous > pd.Timedelta(hours=1):
            result.append(
                GapRecord(
                    "interior_source_gap",
                    symbol,
                    (previous + pd.Timedelta(hours=1)).isoformat(),
                    current.isoformat(),
                    "non-consecutive canonical timestamps",
                )
            )
    if end is not None and values.iloc[-1] + pd.Timedelta(hours=1) < end:
        result.append(
            GapRecord(
                "trailing_source_gap",
                symbol,
                (values.iloc[-1] + pd.Timedelta(hours=1)).isoformat(),
                end.isoformat(),
                "canonical series ends early",
            )
        )
    return result


def validate_canonical_1h(frame: pd.DataFrame, cutoff_exclusive_utc: str, source_entries: list[SourceEntry]) -> None:
    validate_normalized_1h(frame)
    if frame.empty:
        raise AcquisitionInvariantError("Canonical normalized 1H layer is empty")
    cutoff = floor_to_1h(pd.Timestamp(cutoff_exclusive_utc))
    if frame["open_time"].max() >= cutoff or frame["close_time"].max() >= cutoff:
        raise AcquisitionInvariantError("Canonical 1H contains incomplete/post-cutoff bar")
    if "source_key" not in frame.columns or "source_sha256" not in frame.columns:
        raise AcquisitionInvariantError("Canonical 1H rows require raw source lineage")
    allowed = {entry.object_key_or_request for entry in source_entries}
    if not set(frame["source_key"]).issubset(allowed):
        raise AcquisitionInvariantError("Canonical 1H cites source outside frozen inventory")
    if frame.duplicated(["symbol", "open_time"]).any():
        raise AcquisitionInvariantError("Duplicate canonical (symbol, open_time) key")


def classify_raw_conflict(frozen_published_sha256: str, current_published_sha256: str, local_sha256: str) -> str:
    if local_sha256 != frozen_published_sha256:
        return "local_corruption"
    if current_published_sha256 != frozen_published_sha256:
        return "upstream_source_revision"
    return "verified"


def publish_staged(path: str | Path, writer: Callable[[Path], None]) -> str:
    """Publish a complete stage directory atomically with no replacement and a lock."""
    target = Path(path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    lock = target.parent / f".{target.name}.lock"
    lock_fd: int | None = None
    stage: Path | None = None
    try:
        try:
            lock_fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError as exc:
            raise AcquisitionInvariantError(f"Concurrent publication lock exists: {lock}") from exc
        stage = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
        writer(stage)
        marker = stage / "_SUCCESS"
        with marker.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write("complete\n")
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.rename(stage, target)
        except OSError as exc:
            if target.exists():
                raise FileExistsError(target) from exc
            raise
        stage = None
        return str(target)
    finally:
        if stage is not None and stage.exists():
            shutil.rmtree(stage, ignore_errors=True)
        if lock_fd is not None:
            os.close(lock_fd)
            lock.unlink(missing_ok=True)


def _gate_core(name: str, upstream: dict[str, str], checks: dict[str, bool], details: dict[str, Any]) -> dict[str, Any]:
    if not checks or not all(checks.values()):
        raise AcquisitionInvariantError(f"Gate {name} invariant failed: {checks}")
    return {
        "schema_version": "derived-performance-blind-gate-v2",
        "gate": name,
        "status": "passed",
        "upstream": upstream,
        "checks": checks,
        "details": details,
        "performance_aggregate_emitted": False,
    }


def derived_gate(name: str, upstream: dict[str, str], checks: dict[str, bool], details: dict[str, Any]) -> dict[str, Any]:
    core = _gate_core(name, upstream, checks, details)
    return {**core, "gate_id": digest_json(core)}


def require_gate_pass(artifact: dict[str, Any], allowed: set[str], expected_upstream: dict[str, str] | None = None) -> None:
    required = {
        "schema_version",
        "gate",
        "status",
        "upstream",
        "checks",
        "details",
        "performance_aggregate_emitted",
        "gate_id",
    }
    if type(artifact) is not dict or set(artifact) != required:
        raise AcquisitionInvariantError("Gate artifact schema is not exact")
    core = {key: value for key, value in artifact.items() if key != "gate_id"}
    if artifact["schema_version"] != "derived-performance-blind-gate-v2" or artifact["gate_id"] != digest_json(core):
        raise AcquisitionInvariantError("Gate artifact identity is invalid")
    if artifact["gate"] not in allowed or artifact["status"] != "passed" or artifact["performance_aggregate_emitted"]:
        raise AcquisitionInvariantError("Engineering gate is not a passing performance-blind artifact")
    if not artifact["checks"] or not all(value is True for value in artifact["checks"].values()):
        raise AcquisitionInvariantError("Passing gate contains non-passing derived checks")
    if expected_upstream is not None and artifact["upstream"] != expected_upstream:
        raise AcquisitionInvariantError("Gate artifact upstream binding mismatch")


def gate_artifact(*args: Any, **kwargs: Any) -> dict[str, Any]:
    """Former caller-boolean constructor is intentionally disabled on the canonical path."""
    raise AcquisitionInvariantError("Caller-supplied gate PASS construction is prohibited")
