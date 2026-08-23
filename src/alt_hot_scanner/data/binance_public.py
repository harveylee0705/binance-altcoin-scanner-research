from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from alt_hot_scanner.identity import require_binance_token, require_canonical_text

ARCHIVE_HOST = "https://data.binance.vision"
INDEX_HOST = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
_SHA256 = re.compile(r"[0-9a-f]{64}")
_ARCHIVE_KEY = re.compile(
    r"data/futures/um/monthly/klines/"
    r"(?P<symbol>[A-Z0-9]{1,64})/"
    r"(?P<interval>1h)/"
    r"(?P=symbol)-(?P=interval)-(?P<period>20[0-9]{2}-(?:0[1-9]|1[0-2]))\.zip"
)


@dataclass(frozen=True)
class ArchiveObjectIdentity:
    object_key: str
    symbol: str
    interval: str
    period: str
    filename: str


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
    key = require_canonical_text(object_key, "object_key")
    if "\\" in key:
        raise ValueError("object_key must use POSIX separators only")
    match = _ARCHIVE_KEY.fullmatch(key)
    if match is None:
        raise ValueError("object_key is outside the Binance USD-M monthly 1H kline hierarchy")
    symbol = require_binance_token(match.group("symbol"), "object_key symbol")
    return ArchiveObjectIdentity(
        object_key=key,
        symbol=symbol,
        interval=match.group("interval"),
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


def list_archive_symbols() -> list[str]:
    """Discover symbols from the official archive index, including later-delisted objects."""
    prefix = "data/futures/um/monthly/klines/"
    query = urllib.parse.urlencode({"list-type": "2", "prefix": prefix, "delimiter": "/"})
    root = ET.fromstring(_read_url(f"{INDEX_HOST}?{query}"))
    namespace = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    prefixes = [node.text or "" for node in root.findall("s3:CommonPrefixes/s3:Prefix", namespace)]
    symbols = {item.removeprefix(prefix).removesuffix("/") for item in prefixes if item}
    return sorted(require_binance_token(symbol, "archive index symbol") for symbol in symbols)


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
