from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import secrets
import tempfile
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from alt_hot_scanner.identity import (
    require_archive_symbol_identity,
    require_binance_token,
    require_canonical_text,
    require_semantic_contract_identity,
    safe_identity_component,
)
from alt_hot_scanner.utils.numeric import strict_millisecond_timestamp

ARCHIVE_HOST = "https://data.binance.vision"
INDEX_HOST = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
_SHA256 = re.compile(r"[0-9a-f]{64}")
_ARCHIVE_KEY = re.compile(
    r"data/futures/um/(?P<source_kind>monthly|daily)/klines/"
    r"(?P<symbol>[^/\\]{1,128})/"
    r"(?P<interval>1h)/"
    r"(?P=symbol)-(?P=interval)-(?P<period>20[0-9]{2}-(?:0[1-9]|1[0-2])(?:-(?:0[1-9]|[12][0-9]|3[01]))?)\.zip"
)
_DAILY_TRADE_KEY = re.compile(
    r"data/futures/um/daily/trades/"
    r"(?P<symbol>[^/\\]{1,128})/"
    r"(?P=symbol)-trades-(?P<period>20[0-9]{2}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01]))\.zip"
)


@dataclass(frozen=True)
class ArchiveObjectIdentity:
    object_key: str
    symbol: str
    interval: str
    period: str
    filename: str
    source_kind: str = "monthly"


@dataclass(frozen=True)
class DailyTradeArchiveIdentity:
    object_key: str
    symbol: str
    period: str
    filename: str


@dataclass(frozen=True)
class FirstObservedTradeEvidence:
    symbol: str
    archive_object_key: str
    archive_date: str
    published_sha256: str
    computed_sha256: str
    raw_path: str
    original_retrieval_timestamp: str
    earliest_trade_timestamp: str
    parser_version: str
    evidence_status: str


@dataclass(frozen=True)
class ProcessingArchive:
    identity: ArchiveObjectIdentity
    local_path: Path
    sha256: str
    byte_count: int


@dataclass(frozen=True)
class DownloadRecord:
    object_key: str
    url: str
    checksum_url: str
    retrieved_at: str
    byte_count: int
    published_sha256: str
    computed_sha256: str
    checksum_verified: bool
    local_path: str
    payload_source: str


@dataclass(frozen=True)
class ArchiveIndexAudit:
    page_count: int
    returned_prefix_count: int
    returned_key_count: int
    unique_prefix_count: int
    unique_key_count: int
    any_page_truncated: bool
    source_urls: tuple[str, ...]


@dataclass(frozen=True)
class ArchiveIndexListing:
    prefixes: tuple[str, ...]
    keys: tuple[str, ...]
    audit: ArchiveIndexAudit


@dataclass(frozen=True)
class ArchiveSymbolDiscovery:
    symbols: tuple[str, ...]
    quarantined_prefixes: tuple[str, ...]
    audit: ArchiveIndexAudit


@dataclass(frozen=True)
class ArchiveMonthObservation:
    symbol: str
    first_archive_month: str
    last_archive_month: str
    archive_discovery_timestamp: str
    archive_source_url: str
    archive_discovery_provenance: str
    archive_parser_version: str
    index_page_count: int
    returned_key_count: int
    unique_archive_count: int
    any_page_truncated: bool
    observed_archive_object_keys: tuple[str, ...]


class ArchiveAcquisitionError(RuntimeError):
    """Structured failure that preserves acquisition stage and available checksum evidence."""

    def __init__(
        self,
        object_key: str,
        stage: str,
        request_url: str,
        message: str,
        *,
        status_code: int | None = None,
        published_sha256: str | None = None,
        computed_sha256: str | None = None,
        byte_count: int | None = None,
        local_path: str | None = None,
    ) -> None:
        super().__init__(message)
        self.object_key = object_key
        self.stage = stage
        self.request_url = request_url
        self.status_code = status_code
        self.published_sha256 = published_sha256
        self.computed_sha256 = computed_sha256
        self.byte_count = byte_count
        self.local_path = local_path


def validate_archive_object_key(object_key: object) -> ArchiveObjectIdentity:
    """Accept only the frozen Binance USD-M monthly 1H kline object-key grammar."""
    if type(object_key) is not str or not object_key or object_key != object_key.strip():
        raise ValueError("object_key must be nonempty canonical text")
    key = object_key
    if "\\" in key or unicodedata.normalize("NFC", key) != key:
        raise ValueError("object_key must use canonical Unicode and POSIX separators")
    match = _ARCHIVE_KEY.fullmatch(key)
    if match is None:
        raise ValueError("object_key is outside the Binance USD-M monthly 1H kline hierarchy")
    if match.group("source_kind") == "daily" and len(match.group("period")) != 10:
        raise ValueError("Daily kline object keys require a YYYY-MM-DD period")
    if match.group("source_kind") == "monthly" and len(match.group("period")) != 7:
        raise ValueError("Monthly kline object keys require a YYYY-MM period")
    try:
        datetime.strptime(
            match.group("period"),
            "%Y-%m-%d" if len(match.group("period")) == 10 else "%Y-%m",
        ).replace(tzinfo=UTC)
    except ValueError as exc:
        raise ValueError("Object-key period is not a real calendar date") from exc
    symbol = require_archive_symbol_identity(match.group("symbol"), "object_key symbol")
    return ArchiveObjectIdentity(
        object_key=key,
        symbol=symbol,
        interval=match.group("interval"),
        period=match.group("period"),
        filename=key.rsplit("/", 1)[-1],
        source_kind=match.group("source_kind"),
    )


def validate_daily_trade_object_key(object_key: object) -> DailyTradeArchiveIdentity:
    if type(object_key) is not str or not object_key or object_key != object_key.strip():
        raise ValueError("daily trade object_key must be nonempty canonical text")
    if unicodedata.normalize("NFC", object_key) != object_key or "\\" in object_key:
        raise ValueError("daily trade object_key is not canonical or safe")
    key = object_key
    match = _DAILY_TRADE_KEY.fullmatch(key)
    if match is None:
        raise ValueError("object_key is outside the Binance USD-M daily trade hierarchy")
    symbol = require_archive_symbol_identity(match.group("symbol"), "trade archive symbol")
    return DailyTradeArchiveIdentity(
        object_key=key,
        symbol=symbol,
        period=match.group("period"),
        filename=key.rsplit("/", 1)[-1],
    )


def confined_archive_path(raw_root: str | Path, object_key: object) -> Path:
    """Construct and independently prove the canonical destination is under raw_root."""
    identity = validate_archive_object_key(object_key)
    root = Path(raw_root).resolve(strict=False)
    candidate = root.joinpath(*identity.object_key.split("/"))
    resolved = candidate.resolve(strict=False)
    if not resolved.is_relative_to(root):
        raise ValueError("Canonical archive destination resolves outside raw_root")
    return resolved


def sha256_file(path: str | Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    byte_count = 0
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
            byte_count += len(chunk)
    return digest.hexdigest(), byte_count


def parse_checksum_sidecar(payload: bytes, identity: ArchiveObjectIdentity) -> str:
    try:
        checksum_text = payload.decode("utf-8").strip()
    except UnicodeDecodeError as exc:
        raise ValueError("Checksum sidecar is not valid UTF-8") from exc
    match = re.fullmatch(r"([0-9a-fA-F]{64})[ \t]+\*?([^\r\n]+)", checksum_text)
    if match is None or match.group(2) != identity.filename:
        raise ValueError("Checksum sidecar must name the exact archive object")
    return match.group(1).lower()


def collision_resistant_run_id(now: datetime | None = None) -> str:
    current = now or datetime.now(UTC)
    return f"{current.strftime('%Y%m%dT%H%M%S%fZ')}_{os.getpid()}_{secrets.token_hex(8)}"


def _discard_temporary(path: Path | None) -> None:
    if path is None:
        return
    try:
        path.unlink(missing_ok=True)
    except OSError:
        # A noncanonical .tmp/.part artifact is diagnosable and must not mask the primary failure.
        pass


def _write_bytes_exclusive_atomic(path: str | Path, payload: bytes) -> None:
    """Durably install bytes with atomic no-replace semantics on supported filesystems."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=target.parent, prefix=f".{target.name}.", suffix=".tmp"
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, target)
    finally:
        _discard_temporary(temporary)


def write_json_exclusive(path: str | Path, payload: Any) -> None:
    serialized = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
    _write_bytes_exclusive_atomic(path, serialized)


def write_bytes_exclusive(path: str | Path, payload: bytes) -> None:
    _write_bytes_exclusive_atomic(path, payload)


def _read_url(url: str, timeout: int = 60) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "alt-hot-scanner/0.1"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def download_verified_archive(
    object_key: str,
    raw_root: str | Path,
) -> DownloadRecord:
    """Verify temporary bytes, then atomically install without replacing canonical data."""
    try:
        identity = validate_archive_object_key(object_key)
        destination = confined_archive_path(raw_root, object_key)
    except (TypeError, ValueError) as exc:
        raise ArchiveAcquisitionError(
            str(object_key),
            "invalid_object_key",
            ARCHIVE_HOST,
            f"Invalid archive object identity: {exc}",
        ) from exc
    url = f"{ARCHIVE_HOST}/{identity.object_key}"
    checksum_url = f"{url}.CHECKSUM"
    try:
        checksum_payload = _read_url(checksum_url)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            try:
                archive_exists = object_exists(object_key)
            except (OSError, urllib.error.URLError) as probe_exc:
                raise ArchiveAcquisitionError(
                    object_key,
                    "archive_existence_probe",
                    url,
                    f"Could not distinguish missing archive from missing checksum: {probe_exc}",
                ) from probe_exc
            if not archive_exists:
                raise ArchiveAcquisitionError(
                    object_key,
                    "archive_payload",
                    url,
                    "Archive object does not exist",
                    status_code=404,
                ) from exc
        raise ArchiveAcquisitionError(
            object_key,
            "checksum_sidecar",
            checksum_url,
            f"Checksum sidecar request failed with HTTP {exc.code}",
            status_code=exc.code,
        ) from exc
    except (OSError, urllib.error.URLError) as exc:
        raise ArchiveAcquisitionError(
            object_key,
            "checksum_sidecar",
            checksum_url,
            f"Checksum sidecar request failed: {exc}",
        ) from exc
    try:
        published = parse_checksum_sidecar(checksum_payload, identity)
    except ValueError as exc:
        raise ArchiveAcquisitionError(
            object_key,
            "checksum_parse",
            checksum_url,
            str(exc),
        ) from exc

    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination = confined_archive_path(raw_root, object_key)
    except OSError as exc:
        raise ArchiveAcquisitionError(
            object_key,
            "destination_preparation",
            str(destination),
            f"Could not prepare canonical raw destination: {exc}",
            published_sha256=published,
            local_path=str(destination),
        ) from exc
    except ValueError as exc:
        raise ArchiveAcquisitionError(
            object_key,
            "destination_confinement",
            str(destination),
            str(exc),
            published_sha256=published,
            local_path=str(destination),
        ) from exc

    try:
        destination_exists = destination.exists()
    except OSError as exc:
        raise ArchiveAcquisitionError(
            object_key,
            "local_read",
            str(destination),
            f"Could not inspect canonical raw destination: {exc}",
            published_sha256=published,
            local_path=str(destination),
        ) from exc
    if destination_exists:
        try:
            computed, byte_count = sha256_file(destination)
        except OSError as exc:
            raise ArchiveAcquisitionError(
                object_key,
                "local_read",
                str(destination),
                f"Could not hash existing canonical raw object: {exc}",
                published_sha256=published,
                local_path=str(destination),
            ) from exc
        if computed != published:
            raise ArchiveAcquisitionError(
                object_key,
                "local_corruption",
                str(destination),
                f"Existing raw object differs from published checksum: {computed}",
                published_sha256=published,
                computed_sha256=computed,
                byte_count=byte_count,
                local_path=str(destination),
            )
        return DownloadRecord(
            object_key=identity.object_key,
            url=url,
            checksum_url=checksum_url,
            retrieved_at=datetime.now(UTC).isoformat(),
            byte_count=byte_count,
            published_sha256=published,
            computed_sha256=computed,
            checksum_verified=True,
            local_path=str(destination),
            payload_source="existing_local_verified_against_published_checksum",
        )

    try:
        payload = _read_url(url)
    except urllib.error.HTTPError as exc:
        raise ArchiveAcquisitionError(
            object_key,
            "archive_payload",
            url,
            f"Archive request failed with HTTP {exc.code}",
            status_code=exc.code,
            published_sha256=published,
            local_path=str(destination),
        ) from exc
    except (OSError, urllib.error.URLError) as exc:
        raise ArchiveAcquisitionError(
            object_key,
            "archive_payload",
            url,
            f"Archive request failed: {exc}",
            published_sha256=published,
            local_path=str(destination),
        ) from exc

    downloaded_hash = hashlib.sha256(payload).hexdigest()
    temporary_dir = Path(raw_root).resolve(strict=False) / ".partial"
    temporary: Path | None = None
    try:
        temporary_dir.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            dir=temporary_dir, prefix=f".{identity.symbol}.", suffix=".part"
        )
        temporary = Path(temporary_name)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except OSError as exc:
        _discard_temporary(temporary)
        raise ArchiveAcquisitionError(
            object_key,
            "temporary_write",
            str(temporary_dir),
            f"Could not persist downloaded bytes to an isolated temporary file: {exc}",
            published_sha256=published,
            computed_sha256=downloaded_hash,
            byte_count=len(payload),
            local_path=str(destination),
        ) from exc

    try:
        computed, byte_count = sha256_file(temporary)
        if computed != published:
            raise ArchiveAcquisitionError(
                object_key,
                "checksum_mismatch",
                url,
                f"Temporary raw bytes differ from published checksum: {computed}",
                published_sha256=published,
                computed_sha256=computed,
                byte_count=byte_count,
                local_path=str(destination),
            )
        try:
            destination = confined_archive_path(raw_root, object_key)
            os.link(temporary, destination)
            payload_source = "downloaded_verified_then_atomic_no_replace_install"
        except FileExistsError as exc:
            try:
                existing_hash, existing_size = sha256_file(destination)
            except OSError as read_exc:
                raise ArchiveAcquisitionError(
                    object_key,
                    "local_read",
                    str(destination),
                    f"Concurrent destination could not be hashed: {read_exc}",
                    published_sha256=published,
                    computed_sha256=computed,
                    byte_count=byte_count,
                    local_path=str(destination),
                ) from read_exc
            if existing_hash != published:
                raise ArchiveAcquisitionError(
                    object_key,
                    "immutable_path_conflict",
                    str(destination),
                    "Concurrent writer installed different canonical bytes",
                    published_sha256=published,
                    computed_sha256=existing_hash,
                    byte_count=existing_size,
                    local_path=str(destination),
                ) from exc
            byte_count = existing_size
            computed = existing_hash
            payload_source = "concurrent_existing_verified_against_published_checksum"
        except ValueError as exc:
            raise ArchiveAcquisitionError(
                object_key,
                "destination_confinement",
                str(destination),
                str(exc),
                published_sha256=published,
                computed_sha256=computed,
                byte_count=byte_count,
                local_path=str(destination),
            ) from exc
        except OSError as exc:
            raise ArchiveAcquisitionError(
                object_key,
                "install_failure",
                str(destination),
                f"Atomic no-replace raw installation failed: {exc}",
                published_sha256=published,
                computed_sha256=computed,
                byte_count=byte_count,
                local_path=str(destination),
            ) from exc
    except ArchiveAcquisitionError:
        raise
    except OSError as exc:
        raise ArchiveAcquisitionError(
            object_key,
            "temporary_read",
            str(temporary),
            f"Could not independently hash temporary raw bytes: {exc}",
            published_sha256=published,
            computed_sha256=downloaded_hash,
            byte_count=len(payload),
            local_path=str(destination),
        ) from exc
    finally:
        _discard_temporary(temporary)

    return DownloadRecord(
        object_key=identity.object_key,
        url=url,
        checksum_url=checksum_url,
        retrieved_at=datetime.now(UTC).isoformat(),
        byte_count=byte_count,
        published_sha256=published,
        computed_sha256=computed,
        checksum_verified=True,
        local_path=str(destination),
        payload_source=payload_source,
    )


def write_download_manifest(records: list[DownloadRecord], path: str | Path) -> None:
    write_json_exclusive(path, [asdict(record) for record in records])


def validate_manifest_archive(entry: object, raw_root: str | Path) -> ProcessingArchive:
    """Validate an untrusted manifest entry and re-hash current canonical bytes."""
    if type(entry) is not dict:
        raise ArchiveAcquisitionError(
            "<unknown>", "invalid_manifest_entry", "<manifest>", "Manifest entry must be an object"
        )
    object_key = entry.get("object_key")
    try:
        identity = validate_archive_object_key(object_key)
        expected_path = confined_archive_path(raw_root, object_key)
        local_path_text = require_canonical_text(entry.get("local_path"), "local_path")
        supplied_path = Path(local_path_text)
        if not supplied_path.is_absolute():
            raise ValueError("Manifest local_path must be absolute")
        supplied_path = supplied_path.resolve(strict=False)
        root = Path(raw_root).resolve(strict=False)
        if not supplied_path.is_relative_to(root) or supplied_path != expected_path:
            raise ValueError("Manifest local_path is not the canonical path for object_key")
        if entry.get("status") != "verified" or entry.get("checksum_verified") is not True:
            raise ValueError("Manifest entry lacks an exact verified status")
    except (TypeError, ValueError) as exc:
        raise ArchiveAcquisitionError(
            str(object_key),
            "invalid_manifest_entry",
            "<manifest>",
            f"Forged or malformed manifest entry: {exc}",
        ) from exc

    published = entry.get("published_sha256")
    computed_at_download = entry.get("computed_sha256")
    if (
        type(published) is not str
        or _SHA256.fullmatch(published) is None
        or type(computed_at_download) is not str
        or _SHA256.fullmatch(computed_at_download) is None
        or published != computed_at_download
    ):
        raise ArchiveAcquisitionError(
            identity.object_key,
            "checksum_evidence",
            "<manifest>",
            "Manifest requires matching lowercase published and download-time SHA-256 evidence",
            published_sha256=published if type(published) is str else None,
            computed_sha256=computed_at_download if type(computed_at_download) is str else None,
            local_path=str(expected_path),
        )
    checksum_url = f"{ARCHIVE_HOST}/{identity.object_key}.CHECKSUM"
    try:
        current_sidecar = _read_url(checksum_url)
    except urllib.error.HTTPError as exc:
        raise ArchiveAcquisitionError(
            identity.object_key,
            "processing_checksum_sidecar",
            checksum_url,
            f"Processing-time checksum sidecar request failed with HTTP {exc.code}",
            status_code=exc.code,
            published_sha256=published,
            computed_sha256=computed_at_download,
            local_path=str(expected_path),
        ) from exc
    except (OSError, urllib.error.URLError) as exc:
        raise ArchiveAcquisitionError(
            identity.object_key,
            "processing_checksum_sidecar",
            checksum_url,
            f"Processing-time checksum sidecar request failed: {exc}",
            published_sha256=published,
            computed_sha256=computed_at_download,
            local_path=str(expected_path),
        ) from exc
    try:
        trusted_published = parse_checksum_sidecar(current_sidecar, identity)
    except ValueError as exc:
        raise ArchiveAcquisitionError(
            identity.object_key,
            "processing_checksum_parse",
            checksum_url,
            str(exc),
            published_sha256=published,
            computed_sha256=computed_at_download,
            local_path=str(expected_path),
        ) from exc
    if trusted_published != published:
        raise ArchiveAcquisitionError(
            identity.object_key,
            "checksum_evidence",
            checksum_url,
            "Manifest checksum does not match the current published Binance sidecar",
            published_sha256=trusted_published,
            computed_sha256=computed_at_download,
            local_path=str(expected_path),
        )
    try:
        current_hash, byte_count = sha256_file(expected_path)
    except OSError as exc:
        raise ArchiveAcquisitionError(
            identity.object_key,
            "local_read",
            str(expected_path),
            f"Could not hash canonical raw bytes immediately before processing: {exc}",
            published_sha256=published,
            computed_sha256=computed_at_download,
            local_path=str(expected_path),
        ) from exc
    if current_hash != published:
        raise ArchiveAcquisitionError(
            identity.object_key,
            "local_corruption",
            str(expected_path),
            "Current canonical raw bytes do not match the published SHA-256",
            published_sha256=published,
            computed_sha256=current_hash,
            byte_count=byte_count,
            local_path=str(expected_path),
        )
    return ProcessingArchive(identity, expected_path, current_hash, byte_count)


def monthly_kline_key(symbol: str, interval: str, year_month: str) -> str:
    key = (
        f"data/futures/um/monthly/klines/{symbol}/{interval}/{symbol}-{interval}-{year_month}.zip"
    )
    return validate_archive_object_key(key).object_key


def list_archive_index(
    prefix: str,
    *,
    delimiter: str | None = None,
    page_observer: Callable[[int, str, bytes], None] | None = None,
) -> ArchiveIndexListing:
    """Read a complete, internally consistent S3 ListObjectsV2 result."""
    if type(prefix) is not str or not prefix or prefix != prefix.strip():
        raise ValueError("archive index prefix must be nonempty canonical text")
    if unicodedata.normalize("NFC", prefix) != prefix:
        raise ValueError("archive index prefix must use canonical Unicode encoding")
    canonical_prefix = prefix
    allowed_prefix = canonical_prefix.startswith(
        ("data/futures/um/monthly/klines/", "data/futures/um/daily/klines/", "data/futures/um/daily/trades/")
    ) and (not canonical_prefix.startswith("data/futures/um/daily/trades/") or "\\" not in canonical_prefix)
    if not allowed_prefix or "\\" in canonical_prefix:
        raise ValueError("Archive index prefix is outside an approved Binance USD-M hierarchy")
    if delimiter not in {None, "/"}:
        raise ValueError("Archive index delimiter must be '/' or omitted")

    namespace_uri = "http://s3.amazonaws.com/doc/2006-03-01/"
    namespace = {"s3": namespace_uri}
    continuation_token: str | None = None
    seen_tokens: set[str] = set()
    prefixes: list[str] = []
    keys: list[str] = []
    source_urls: list[str] = []
    any_truncated = False

    while True:
        parameters = {"list-type": "2", "prefix": canonical_prefix}
        if delimiter is not None:
            parameters["delimiter"] = delimiter
        if continuation_token is not None:
            parameters["continuation-token"] = continuation_token
        query = urllib.parse.urlencode(parameters)
        url = f"{INDEX_HOST}?{query}"
        source_urls.append(url)
        payload = _read_url(url)
        if page_observer is not None:
            page_observer(len(source_urls), url, payload)
        try:
            root = ET.fromstring(payload)
        except ET.ParseError as exc:
            raise ValueError(f"Malformed archive-index XML on page {len(source_urls)}") from exc
        if root.tag != f"{{{namespace_uri}}}ListBucketResult":
            raise ValueError("Archive-index XML has an unexpected root or namespace")

        truncated_nodes = root.findall("s3:IsTruncated", namespace)
        if len(truncated_nodes) != 1 or truncated_nodes[0].text not in {"true", "false"}:
            raise ValueError("Archive-index page requires one canonical IsTruncated value")
        truncated = truncated_nodes[0].text == "true"
        any_truncated = any_truncated or truncated

        page_prefixes = [
            node.text
            for node in root.findall("s3:CommonPrefixes/s3:Prefix", namespace)
        ]
        page_keys = [node.text for node in root.findall("s3:Contents/s3:Key", namespace)]
        if any(value is None or value == "" for value in [*page_prefixes, *page_keys]):
            raise ValueError("Archive-index page contains an empty prefix or key")
        if any(not value.startswith(canonical_prefix) for value in [*page_prefixes, *page_keys]):
            raise ValueError("Archive-index page returned an object outside the requested prefix")

        key_count_nodes = root.findall("s3:KeyCount", namespace)
        if len(key_count_nodes) > 1:
            raise ValueError("Archive-index page contains duplicate KeyCount fields")
        if key_count_nodes:
            try:
                key_count = int(key_count_nodes[0].text or "")
            except ValueError as exc:
                raise ValueError("Archive-index KeyCount is malformed") from exc
            if key_count != len(page_prefixes) + len(page_keys):
                raise ValueError("Archive-index KeyCount disagrees with returned entries")

        prefixes.extend(value for value in page_prefixes if value is not None)
        keys.extend(value for value in page_keys if value is not None)
        token_nodes = root.findall("s3:NextContinuationToken", namespace)
        if len(token_nodes) > 1:
            raise ValueError("Archive-index page contains duplicate continuation tokens")
        next_token = token_nodes[0].text if token_nodes else None
        if truncated:
            if next_token is None or not next_token.strip():
                raise ValueError("Truncated archive-index page has no usable continuation token")
            next_token = require_canonical_text(next_token, "archive continuation token")
            if next_token in seen_tokens:
                raise ValueError("Archive-index pagination repeated a continuation token")
            seen_tokens.add(next_token)
            continuation_token = next_token
            continue
        if next_token is not None and next_token.strip():
            raise ValueError("Untruncated archive-index page unexpectedly supplied a next token")
        break

    unique_prefixes = tuple(sorted(set(prefixes)))
    unique_keys = tuple(sorted(set(keys)))
    return ArchiveIndexListing(
        prefixes=unique_prefixes,
        keys=unique_keys,
        audit=ArchiveIndexAudit(
            page_count=len(source_urls),
            returned_prefix_count=len(prefixes),
            returned_key_count=len(keys),
            unique_prefix_count=len(unique_prefixes),
            unique_key_count=len(unique_keys),
            any_page_truncated=any_truncated,
            source_urls=tuple(source_urls),
        ),
    )


def discover_earliest_daily_trade_archive(
    symbol: str,
    *,
    page_observer: Callable[[int, str, bytes], None] | None = None,
) -> DailyTradeArchiveIdentity:
    semantic_symbol = require_semantic_contract_identity(symbol, "trade probe symbol")
    prefix = f"data/futures/um/daily/trades/{semantic_symbol}/"
    listing = list_archive_index(prefix, page_observer=page_observer)
    identities: dict[str, DailyTradeArchiveIdentity] = {}
    for key in listing.keys:
        candidate = key.removesuffix(".CHECKSUM")
        try:
            identity = validate_daily_trade_object_key(candidate)
        except ValueError as exc:
            raise ValueError(f"Unexpected object in daily trade listing for {symbol}") from exc
        if identity.symbol != semantic_symbol:
            raise ValueError("Daily trade listing returned a mismatched symbol")
        if not key.endswith(".CHECKSUM"):
            identities[identity.object_key] = identity
    if not identities:
        raise ValueError(f"No daily trade archives found for {semantic_symbol}")
    return min(identities.values(), key=lambda value: (value.period, value.object_key))


def parse_earliest_trade_timestamp(path: str | Path) -> pd.Timestamp:
    """Parse the true minimum timestamp from one checksum-verified Binance trade ZIP."""
    try:
        with zipfile.ZipFile(path) as archive:
            members = [item for item in archive.infolist() if not item.is_dir()]
            if len(members) != 1 or not members[0].filename.endswith(".csv"):
                raise ValueError("Trade archive must contain exactly one CSV")
            minimum: int | None = None
            with archive.open(members[0]) as raw:
                reader = csv.reader(io.TextIOWrapper(raw, encoding="utf-8-sig", newline=""))
                for row_number, row in enumerate(reader, start=1):
                    if row_number == 1 and row and row[0].strip().lower() == "id":
                        continue
                    if len(row) < 5:
                        raise ValueError(f"Trade row {row_number} has too few fields")
                    timestamp = strict_millisecond_timestamp(
                        row[4], f"trade row {row_number} timestamp"
                    )
                    minimum = timestamp if minimum is None else min(minimum, timestamp)
    except zipfile.BadZipFile as exc:
        raise ValueError("Trade archive is not a valid ZIP") from exc
    if minimum is None:
        raise ValueError("Trade archive contains no trades")
    return pd.to_datetime(minimum, unit="ms", utc=True)


def acquire_first_observed_trade(
    symbol: str,
    raw_root: str | Path,
    *,
    page_observer: Callable[[int, str, bytes], None] | None = None,
) -> FirstObservedTradeEvidence:
    """Download only the earliest daily trade ZIP and verify its official SHA-256."""
    identity = discover_earliest_daily_trade_archive(symbol, page_observer=page_observer)
    encoded_key = urllib.parse.quote(identity.object_key, safe="/")
    url = f"{ARCHIVE_HOST}/{encoded_key}"
    checksum_url = f"{url}.CHECKSUM"
    checksum_payload = _read_url(checksum_url)
    published = parse_checksum_sidecar(checksum_payload, identity)  # type: ignore[arg-type]
    destination = (
        Path(raw_root).resolve(strict=False)
        / "first_observed_trades"
        / safe_identity_component(identity.symbol)
        / "earliest_daily_trade.zip"
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        computed, _ = sha256_file(destination)
        if computed != published:
            raise ValueError("Existing first-trade evidence differs from published checksum")
        retrieved_at = datetime.fromtimestamp(destination.stat().st_mtime, UTC).isoformat()
    else:
        payload = _read_url(url)
        computed = hashlib.sha256(payload).hexdigest()
        if computed != published:
            raise ValueError("First-trade archive differs from published checksum")
        _write_bytes_exclusive_atomic(destination, payload)
        retrieved_at = datetime.now(UTC).isoformat()
    earliest = parse_earliest_trade_timestamp(destination)
    return FirstObservedTradeEvidence(
        symbol=identity.symbol,
        archive_object_key=identity.object_key,
        archive_date=identity.period,
        published_sha256=published,
        computed_sha256=computed,
        raw_path=str(destination.resolve()),
        original_retrieval_timestamp=retrieved_at,
        earliest_trade_timestamp=earliest.isoformat(),
        parser_version="binance-usdm-daily-trade-first-observation-v1",
        evidence_status="checksum_verified_official_binance_futures_trade",
    )


def acquire_episode_first_observed_trade(
    symbol: str,
    archive_object_key: str,
    raw_root: str | Path,
    *,
    episode_id: str,
) -> dict[str, Any]:
    """Acquire one reviewed post-gap daily trade archive and derive its first trade."""
    identity = validate_daily_trade_object_key(archive_object_key)
    if identity.symbol != require_archive_symbol_identity(symbol, "episode trade symbol"):
        raise ValueError("Episode trade archive has the wrong symbol")
    if episode_id != f"{symbol}:{episode_id.rsplit(':', 1)[-1]}":
        raise ValueError("Episode identifier is not bound to the archive symbol")
    encoded_key = urllib.parse.quote(identity.object_key, safe="/")
    url = f"{ARCHIVE_HOST}/{encoded_key}"
    checksum_payload = _read_url(f"{url}.CHECKSUM")
    published = parse_checksum_sidecar(checksum_payload, identity)  # type: ignore[arg-type]
    destination = (
        Path(raw_root).resolve(strict=False)
        / "episode_first_observed_trades"
        / safe_identity_component(identity.symbol)
        / f"episode_{episode_id.rsplit(':', 1)[-1]}_{identity.period}.zip"
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        computed, _ = sha256_file(destination)
        if computed != published:
            raise ValueError("Existing episode trade evidence differs from published checksum")
        retrieved_at = datetime.fromtimestamp(destination.stat().st_mtime, UTC).isoformat()
    else:
        payload = _read_url(url)
        computed = hashlib.sha256(payload).hexdigest()
        if computed != published:
            raise ValueError("Episode trade archive differs from published checksum")
        _write_bytes_exclusive_atomic(destination, payload)
        retrieved_at = datetime.now(UTC).isoformat()
    return {
        "symbol": identity.symbol,
        "lifecycle_episode_id": episode_id,
        "archive_object_key": identity.object_key,
        "archive_date": identity.period,
        "published_sha256": published,
        "computed_sha256": computed,
        "raw_path": str(destination.resolve()),
        "original_retrieval_timestamp": retrieved_at,
        "earliest_trade_timestamp": parse_earliest_trade_timestamp(destination).isoformat(),
        "parser_version": "binance-usdm-daily-trade-episode-first-observation-v1",
        "evidence_status": "checksum_verified_official_binance_futures_trade",
    }


def verify_first_observed_trade_record(row: dict[str, Any]) -> None:
    """Re-hash and reparse one persisted first-trade primitive for safe process parallelism."""
    required = {
        "symbol",
        "published_sha256",
        "computed_sha256",
        "raw_path",
        "earliest_trade_timestamp",
    }
    if type(row) is not dict or not required.issubset(row):
        raise ValueError("First-observed-trade record is malformed")
    require_archive_symbol_identity(row["symbol"], "first trade record symbol")
    computed, _ = sha256_file(row["raw_path"])
    if computed != row["computed_sha256"] or computed != row["published_sha256"]:
        raise ValueError("First-observed-trade checkpoint checksum mismatch")
    if parse_earliest_trade_timestamp(row["raw_path"]).isoformat() != row[
        "earliest_trade_timestamp"
    ]:
        raise ValueError("First-observed-trade checkpoint timestamp mismatch")


def discover_archive_symbol_candidates(
    *, page_observer: Callable[[int, str, bytes], None] | None = None
) -> ArchiveSymbolDiscovery:
    """Validate every symbol prefix, retaining noncanonical identities in quarantine."""
    prefix = "data/futures/um/monthly/klines/"
    listing = list_archive_index(prefix, delimiter="/", page_observer=page_observer)
    symbols: list[str] = []
    quarantined: list[str] = []
    for item in listing.prefixes:
        if not item.startswith(prefix) or not item.endswith("/"):
            raise ValueError("Archive symbol prefix has an invalid hierarchy")
        relative = item[len(prefix) : -1]
        if "/" in relative:
            raise ValueError("Archive symbol prefix contains unexpected nesting")
        try:
            symbols.append(require_binance_token(relative, "archive index symbol"))
        except ValueError:
            quarantined.append(relative)
    unique_symbols = sorted(set(symbols))
    audit = ArchiveIndexAudit(
        page_count=listing.audit.page_count,
        returned_prefix_count=listing.audit.returned_prefix_count,
        returned_key_count=listing.audit.returned_key_count,
        unique_prefix_count=listing.audit.unique_prefix_count,
        unique_key_count=listing.audit.unique_key_count,
        any_page_truncated=listing.audit.any_page_truncated,
        source_urls=listing.audit.source_urls,
    )
    if len(unique_symbols) + len(set(quarantined)) != audit.unique_prefix_count:
        raise ValueError("Archive symbol validation changed the unique-prefix count")
    return ArchiveSymbolDiscovery(
        symbols=tuple(unique_symbols),
        quarantined_prefixes=tuple(sorted(set(quarantined))),
        audit=audit,
    )


def discover_archive_symbols(
    *, page_observer: Callable[[int, str, bytes], None] | None = None
) -> tuple[list[str], ArchiveIndexAudit]:
    """Return canonical symbols, failing if any official prefix cannot be represented safely."""
    discovery = discover_archive_symbol_candidates(page_observer=page_observer)
    if discovery.quarantined_prefixes:
        raise ValueError(
            "Archive discovery contains noncanonical symbol identities; use the lifecycle "
            "candidate workflow to retain them in quarantine"
        )
    return list(discovery.symbols), discovery.audit


def list_archive_symbols() -> list[str]:
    """Compatibility wrapper returning complete historical archive symbol discovery."""
    return discover_archive_symbols()[0]


def discover_archive_months(
    symbol: str,
    *,
    discovered_at: datetime | None = None,
    page_observer: Callable[[int, str, bytes], None] | None = None,
) -> ArchiveMonthObservation:
    """Capture observed monthly 1H bounds without calling them lifecycle timestamps."""
    canonical_symbol = require_archive_symbol_identity(symbol, "archive symbol")
    prefix = f"data/futures/um/monthly/klines/{canonical_symbol}/1h/"
    listing = list_archive_index(prefix, page_observer=page_observer)
    identities: dict[str, ArchiveObjectIdentity] = {}
    for returned_key in listing.keys:
        is_checksum = returned_key.endswith(".CHECKSUM")
        object_key = returned_key.removesuffix(".CHECKSUM") if is_checksum else returned_key
        try:
            identity = validate_archive_object_key(object_key)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Unexpected object in monthly 1H archive listing for {canonical_symbol}"
            ) from exc
        if identity.symbol != canonical_symbol:
            raise ValueError("Archive month listing returned a mismatched symbol")
        if not is_checksum:
            identities[identity.object_key] = identity
    if not identities:
        raise ValueError(f"No monthly 1H archives found for {canonical_symbol}")
    periods = sorted(identity.period for identity in identities.values())
    timestamp = discovered_at or datetime.now(UTC)
    return ArchiveMonthObservation(
        symbol=canonical_symbol,
        first_archive_month=periods[0],
        last_archive_month=periods[-1],
        archive_discovery_timestamp=timestamp.isoformat(),
        archive_source_url=INDEX_HOST,
        archive_discovery_provenance=(
            "official_binance_public_s3_listobjectsv2_observed_data_bound_not_lifecycle_event"
        ),
        archive_parser_version="binance-s3-listobjectsv2-v1",
        index_page_count=listing.audit.page_count,
        returned_key_count=listing.audit.returned_key_count,
        unique_archive_count=len(identities),
        any_page_truncated=listing.audit.any_page_truncated,
        observed_archive_object_keys=tuple(sorted(identities)),
    )


def observed_zip_keys_from_index_snapshots(
    paths: list[str] | tuple[str, ...], symbol: str
) -> tuple[str, ...]:
    """Recover exact ZIP objects from preserved official index pages; sidecars do not count."""
    canonical_symbol = require_archive_symbol_identity(symbol, "archive symbol")
    namespace_uri = "http://s3.amazonaws.com/doc/2006-03-01/"
    keys: set[str] = set()
    for raw_path in paths:
        try:
            root = ET.fromstring(Path(raw_path).read_bytes())
        except (OSError, ET.ParseError) as exc:
            raise ValueError(f"Cannot parse preserved archive index snapshot {raw_path}") from exc
        if root.tag != f"{{{namespace_uri}}}ListBucketResult":
            raise ValueError("Preserved archive index snapshot has an unexpected namespace")
        for node in root.findall(f"{{{namespace_uri}}}Contents/{{{namespace_uri}}}Key"):
            key = node.text
            if not key or key.endswith(".CHECKSUM"):
                continue
            identity = validate_archive_object_key(key)
            if identity.symbol != canonical_symbol:
                raise ValueError("Preserved archive index snapshot contains a mismatched symbol")
            keys.add(identity.object_key)
    if not keys:
        raise ValueError(f"No actual ZIP objects found in preserved index pages for {symbol}")
    return tuple(sorted(keys))


def observed_daily_trade_keys_from_index_snapshots(
    paths: list[str] | tuple[str, ...], symbol: str
) -> tuple[str, ...]:
    """Recover exact daily trade ZIP identities from complete preserved S3 index pages."""
    semantic_symbol = require_semantic_contract_identity(symbol, "daily trade index symbol")
    namespace_uri = "http://s3.amazonaws.com/doc/2006-03-01/"
    keys: set[str] = set()
    for raw_path in paths:
        try:
            root = ET.fromstring(Path(raw_path).read_bytes())
        except (OSError, ET.ParseError) as exc:
            raise ValueError(f"Cannot parse preserved daily trade index {raw_path}") from exc
        if root.tag != f"{{{namespace_uri}}}ListBucketResult":
            raise ValueError("Preserved daily trade index has an unexpected namespace")
        truncated = root.findall(f"{{{namespace_uri}}}IsTruncated")
        if len(truncated) != 1 or truncated[0].text not in {"true", "false"}:
            raise ValueError("Daily trade index has an invalid truncation marker")
        for node in root.findall(f"{{{namespace_uri}}}Contents/{{{namespace_uri}}}Key"):
            key = node.text
            if not key or key.endswith(".CHECKSUM"):
                continue
            identity = validate_daily_trade_object_key(key)
            if identity.symbol != semantic_symbol:
                raise ValueError("Daily trade index contains a mismatched symbol")
            keys.add(identity.object_key)
    if not keys:
        raise ValueError(f"No daily trade ZIP objects found for {semantic_symbol}")
    return tuple(sorted(keys))


def fetch_exchange_info_snapshot(path: str | Path) -> dict:
    """Capture current exchange information verbatim; never treat it as historical membership."""
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    payload = _read_url(url)
    target = Path(path)
    _write_bytes_exclusive_atomic(target, payload)
    return json.loads(payload)


def object_exists(object_key: str) -> bool:
    identity = validate_archive_object_key(object_key)
    request = urllib.request.Request(
        f"{ARCHIVE_HOST}/{identity.object_key}",
        method="HEAD",
        headers={"User-Agent": "alt-hot-scanner/0.1"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30):
            return True
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return False
        raise
