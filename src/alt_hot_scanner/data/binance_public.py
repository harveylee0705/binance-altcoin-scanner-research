from __future__ import annotations

import hashlib
import json
import re
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

ARCHIVE_HOST = "https://data.binance.vision"
INDEX_HOST = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"


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
    ) -> None:
        super().__init__(message)
        self.object_key = object_key
        self.stage = stage
        self.request_url = request_url
        self.status_code = status_code
        self.published_sha256 = published_sha256
        self.computed_sha256 = computed_sha256


def _read_url(url: str, timeout: int = 60) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "alt-hot-scanner/0.1"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def download_verified_archive(
    object_key: str,
    raw_root: str | Path,
) -> DownloadRecord:
    """Acquire a verified archive without ever overwriting an existing raw path."""
    url = f"{ARCHIVE_HOST}/{object_key.lstrip('/')}"
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
        checksum_text = checksum_payload.decode("utf-8").strip()
    except UnicodeDecodeError as exc:
        raise ArchiveAcquisitionError(
            object_key,
            "checksum_parse",
            checksum_url,
            "Checksum sidecar is not valid UTF-8",
        ) from exc
    match = re.fullmatch(r"([0-9a-fA-F]{64})\s+\*?(.+)", checksum_text)
    if not match:
        raise ArchiveAcquisitionError(
            object_key,
            "checksum_parse",
            checksum_url,
            f"Unrecognized checksum sidecar: {checksum_text!r}",
        )
    published = match.group(1).lower()

    destination = Path(raw_root) / object_key
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        payload = destination.read_bytes()
        payload_source = "existing_local_verified_against_published_checksum"
    else:
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
            ) from exc
        except (OSError, urllib.error.URLError) as exc:
            raise ArchiveAcquisitionError(
                object_key,
                "archive_payload",
                url,
                f"Archive request failed: {exc}",
                published_sha256=published,
            ) from exc
        payload_source = "downloaded_http_200"
    computed = hashlib.sha256(payload).hexdigest()
    if computed != published:
        raise ArchiveAcquisitionError(
            object_key,
            "checksum_verification",
            url,
            f"Checksum mismatch: published={published}, computed={computed}",
            published_sha256=published,
            computed_sha256=computed,
        )
    if not destination.exists():
        try:
            with destination.open("xb") as handle:
                handle.write(payload)
        except FileExistsError as exc:
            existing_hash = hashlib.sha256(destination.read_bytes()).hexdigest()
            if existing_hash != published:
                raise ArchiveAcquisitionError(
                    object_key,
                    "immutable_path_conflict",
                    str(destination),
                    "Raw destination appeared concurrently with different bytes",
                    published_sha256=published,
                    computed_sha256=existing_hash,
                ) from exc
    return DownloadRecord(
        object_key=object_key,
        url=url,
        checksum_url=checksum_url,
        retrieved_at=datetime.now(UTC).isoformat(),
        byte_count=len(payload),
        published_sha256=published,
        computed_sha256=computed,
        checksum_verified=True,
        local_path=str(destination),
        payload_source=payload_source,
    )


def write_download_manifest(records: list[DownloadRecord], path: str | Path) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps([asdict(record) for record in records], indent=2, sort_keys=True),
        encoding="utf-8",
    )


def monthly_kline_key(symbol: str, interval: str, year_month: str) -> str:
    return (
        f"data/futures/um/monthly/klines/{symbol}/{interval}/{symbol}-{interval}-{year_month}.zip"
    )


def list_archive_symbols() -> list[str]:
    """Discover symbols from the official archive index, including later-delisted objects."""
    prefix = "data/futures/um/monthly/klines/"
    query = urllib.parse.urlencode({"list-type": "2", "prefix": prefix, "delimiter": "/"})
    root = ET.fromstring(_read_url(f"{INDEX_HOST}?{query}"))
    namespace = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    prefixes = [node.text or "" for node in root.findall("s3:CommonPrefixes/s3:Prefix", namespace)]
    return sorted({item.removeprefix(prefix).strip("/") for item in prefixes if item})


def fetch_exchange_info_snapshot(path: str | Path) -> dict:
    """Capture current exchange information verbatim; never treat it as historical membership."""
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    payload = _read_url(url)
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        raise FileExistsError(f"Raw metadata snapshots are append-only: {target}")
    target.write_bytes(payload)
    return json.loads(payload)


def object_exists(object_key: str) -> bool:
    request = urllib.request.Request(
        f"{ARCHIVE_HOST}/{object_key.lstrip('/')}",
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
