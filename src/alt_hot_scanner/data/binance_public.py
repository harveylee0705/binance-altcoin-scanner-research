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
    retrieved_at: str
    byte_count: int
    published_sha256: str
    computed_sha256: str
    checksum_verified: bool
    local_path: str


def _read_url(url: str, timeout: int = 60) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "alt-hot-scanner/0.1"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def download_verified_archive(
    object_key: str,
    raw_root: str | Path,
    *,
    overwrite: bool = False,
) -> DownloadRecord:
    """Download a Binance archive and verify its published SHA-256 sidecar."""
    url = f"{ARCHIVE_HOST}/{object_key.lstrip('/')}"
    checksum_url = f"{url}.CHECKSUM"
    checksum_text = _read_url(checksum_url).decode("utf-8").strip()
    match = re.fullmatch(r"([0-9a-fA-F]{64})\s+\*?(.+)", checksum_text)
    if not match:
        raise ValueError(f"Unrecognized checksum sidecar for {object_key}: {checksum_text!r}")
    published = match.group(1).lower()

    destination = Path(raw_root) / object_key
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and not overwrite:
        payload = destination.read_bytes()
    else:
        payload = _read_url(url)
        destination.write_bytes(payload)
    computed = hashlib.sha256(payload).hexdigest()
    if computed != published:
        raise ValueError(
            f"Checksum mismatch for {object_key}: published={published}, computed={computed}"
        )
    return DownloadRecord(
        object_key=object_key,
        url=url,
        retrieved_at=datetime.now(UTC).isoformat(),
        byte_count=len(payload),
        published_sha256=published,
        computed_sha256=computed,
        checksum_verified=True,
        local_path=str(destination),
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
