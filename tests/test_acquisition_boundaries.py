from __future__ import annotations

import hashlib
import json
import os
import subprocess
import threading
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

import alt_hot_scanner.data.binance_public as public_data
from alt_hot_scanner.data.binance_public import (
    ArchiveAcquisitionError,
    confined_archive_path,
    download_verified_archive,
    validate_archive_object_key,
    validate_manifest_archive,
    write_json_exclusive,
)

KEY = "data/futures/um/monthly/klines/ETHUSDT/1h/ETHUSDT-1h-2023-06.zip"
FILENAME = "ETHUSDT-1h-2023-06.zip"


def _sidecar(digest: str) -> bytes:
    return f"{digest}  {FILENAME}\n".encode()


def _manifest_entry(raw_root: Path, payload: bytes) -> dict:
    digest = hashlib.sha256(payload).hexdigest()
    destination = confined_archive_path(raw_root, KEY)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(payload)
    return {
        "object_key": KEY,
        "status": "verified",
        "checksum_verified": True,
        "published_sha256": digest,
        "computed_sha256": digest,
        "local_path": str(destination),
    }


@pytest.mark.parametrize(
    "key",
    [
        f"/{KEY}",
        f"C:/{KEY}",
        f"../{KEY}",
        KEY.replace("/monthly/", "/monthly/../monthly/"),
        KEY.replace("/klines/", "/klines/./"),
        KEY.replace("/", "\\"),
        KEY.replace("monthly/klines", "daily/klines"),
        KEY.replace("/1h/", "/4h/"),
        KEY.replace(FILENAME, "BTCUSDT-1h-2023-06.zip"),
        KEY.replace("2023-06", "2023-13"),
        KEY.replace("data/futures/um", "data/spot"),
        KEY.replace("/ETHUSDT/", "//ETHUSDT/"),
    ],
)
def test_archive_object_key_rejects_traversal_and_invalid_hierarchy(key: str) -> None:
    with pytest.raises(ValueError):
        validate_archive_object_key(key)


def test_resolved_destination_outside_raw_root_is_rejected(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    outside = tmp_path / "outside"
    raw_root.mkdir()
    outside.mkdir()
    link = raw_root / "data"
    if os.name == "nt":
        result = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(link), str(outside)],
            capture_output=True,
            check=False,
            text=True,
        )
        assert result.returncode == 0, result.stderr
    else:
        link.symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="outside raw_root"):
        confined_archive_path(raw_root, KEY)


def test_forged_manifest_path_cannot_escape_or_redirect_processing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payload = b"trusted"
    entry = _manifest_entry(tmp_path / "raw", payload)
    outside = tmp_path / "outside.zip"
    outside.write_bytes(payload)
    entry["local_path"] = str(outside.resolve())
    monkeypatch.setattr(public_data, "_read_url", lambda _: _sidecar(hashlib.sha256(payload).hexdigest()))
    with pytest.raises(ArchiveAcquisitionError, match="canonical path") as error:
        validate_manifest_archive(entry, tmp_path / "raw")
    assert error.value.stage == "invalid_manifest_entry"


def test_processing_rehash_rejects_corruption_after_verified_download(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    original = b"published bytes"
    entry = _manifest_entry(tmp_path, original)
    Path(entry["local_path"]).write_bytes(b"corrupted after download")
    monkeypatch.setattr(public_data, "_read_url", lambda _: _sidecar(entry["published_sha256"]))
    with pytest.raises(ArchiveAcquisitionError) as error:
        validate_manifest_archive(entry, tmp_path)
    assert error.value.stage == "local_corruption"
    assert error.value.computed_sha256 == hashlib.sha256(b"corrupted after download").hexdigest()


def test_forged_manifest_hashes_cannot_replace_published_checksum(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    published_payload = b"published"
    forged_payload = b"forged"
    entry = _manifest_entry(tmp_path, forged_payload)
    published = hashlib.sha256(published_payload).hexdigest()
    monkeypatch.setattr(public_data, "_read_url", lambda _: _sidecar(published))
    with pytest.raises(ArchiveAcquisitionError) as error:
        validate_manifest_archive(entry, tmp_path)
    assert error.value.stage == "checksum_evidence"
    assert error.value.published_sha256 == published


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("published_sha256", None),
        ("published_sha256", "not-a-hash"),
        ("published_sha256", "A" * 64),
        ("computed_sha256", None),
        ("computed_sha256", "0" * 64),
        ("checksum_verified", "true"),
        ("status", " verified"),
    ],
)
def test_missing_or_malformed_manifest_checksum_evidence_fails_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, field: str, value: object
) -> None:
    entry = _manifest_entry(tmp_path, b"trusted")
    entry[field] = value
    monkeypatch.setattr(public_data, "_read_url", lambda _: _sidecar(entry["published_sha256"]))
    with pytest.raises(ArchiveAcquisitionError) as error:
        validate_manifest_archive(entry, tmp_path)
    assert error.value.stage in {"invalid_manifest_entry", "checksum_evidence"}


def test_processing_distinguishes_unavailable_and_malformed_checksum_sidecars(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    entry = _manifest_entry(tmp_path, b"trusted")
    http_error = urllib.error.HTTPError("checksum", 404, "missing", None, None)
    monkeypatch.setattr(public_data, "_read_url", lambda _: (_ for _ in ()).throw(http_error))
    with pytest.raises(ArchiveAcquisitionError) as unavailable:
        validate_manifest_archive(entry, tmp_path)
    assert unavailable.value.stage == "processing_checksum_sidecar"

    monkeypatch.setattr(public_data, "_read_url", lambda _: b"malformed")
    with pytest.raises(ArchiveAcquisitionError) as malformed:
        validate_manifest_archive(entry, tmp_path)
    assert malformed.value.stage == "processing_checksum_parse"


def test_failed_temporary_write_leaves_no_canonical_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payload = b"complete remote bytes"
    digest = hashlib.sha256(payload).hexdigest()
    monkeypatch.setattr(
        public_data,
        "_read_url",
        lambda url: _sidecar(digest) if url.endswith(".CHECKSUM") else payload,
    )
    monkeypatch.setattr(public_data.tempfile, "mkstemp", lambda **_: (_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(ArchiveAcquisitionError) as error:
        download_verified_archive(KEY, tmp_path)
    assert error.value.stage == "temporary_write"
    assert error.value.published_sha256 == error.value.computed_sha256 == digest
    assert not confined_archive_path(tmp_path, KEY).exists()
    assert not any((tmp_path / ".partial").iterdir())


def test_failed_atomic_install_leaves_no_canonical_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payload = b"complete verified bytes"
    digest = hashlib.sha256(payload).hexdigest()
    monkeypatch.setattr(
        public_data,
        "_read_url",
        lambda url: _sidecar(digest) if url.endswith(".CHECKSUM") else payload,
    )
    monkeypatch.setattr(public_data.os, "link", lambda *_: (_ for _ in ()).throw(OSError("install failed")))
    with pytest.raises(ArchiveAcquisitionError) as error:
        download_verified_archive(KEY, tmp_path)
    assert error.value.stage == "install_failure"
    assert not confined_archive_path(tmp_path, KEY).exists()
    assert not any((tmp_path / ".partial").iterdir())


def test_download_checksum_mismatch_is_distinct_and_never_installed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    expected = hashlib.sha256(b"expected").hexdigest()
    monkeypatch.setattr(
        public_data,
        "_read_url",
        lambda url: _sidecar(expected) if url.endswith(".CHECKSUM") else b"wrong payload",
    )
    with pytest.raises(ArchiveAcquisitionError) as error:
        download_verified_archive(KEY, tmp_path)
    assert error.value.stage == "checksum_mismatch"
    assert error.value.published_sha256 == expected
    assert error.value.computed_sha256 == hashlib.sha256(b"wrong payload").hexdigest()
    assert not confined_archive_path(tmp_path, KEY).exists()
    assert not any((tmp_path / ".partial").iterdir())


def test_existing_local_read_failure_preserves_published_checksum(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payload = b"canonical"
    digest = hashlib.sha256(payload).hexdigest()
    destination = confined_archive_path(tmp_path, KEY)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(payload)
    monkeypatch.setattr(public_data, "_read_url", lambda _: _sidecar(digest))
    monkeypatch.setattr(
        public_data,
        "sha256_file",
        lambda _: (_ for _ in ()).throw(PermissionError("read denied")),
    )
    with pytest.raises(ArchiveAcquisitionError) as error:
        download_verified_archive(KEY, tmp_path)
    assert error.value.stage == "local_read"
    assert error.value.published_sha256 == digest
    assert destination.read_bytes() == payload


def test_concurrent_download_installation_is_no_replace_and_both_verify(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payload = b"same verified bytes from both workers"
    digest = hashlib.sha256(payload).hexdigest()
    barrier = threading.Barrier(2)

    def fake_read(url: str) -> bytes:
        if url.endswith(".CHECKSUM"):
            return _sidecar(digest)
        barrier.wait(timeout=5)
        return payload

    monkeypatch.setattr(public_data, "_read_url", fake_read)
    with ThreadPoolExecutor(max_workers=2) as executor:
        records = list(executor.map(lambda _: download_verified_archive(KEY, tmp_path), range(2)))
    assert {record.computed_sha256 for record in records} == {digest}
    assert {record.payload_source for record in records} == {
        "downloaded_verified_then_atomic_no_replace_install",
        "concurrent_existing_verified_against_published_checksum",
    }
    assert confined_archive_path(tmp_path, KEY).read_bytes() == payload


def test_exclusive_manifest_creation_never_overwrites_collision(tmp_path: Path) -> None:
    target = tmp_path / "manifest.json"
    write_json_exclusive(target, {"run": 1})
    before = target.read_bytes()
    with pytest.raises(FileExistsError):
        write_json_exclusive(target, {"run": 2})
    assert target.read_bytes() == before


def test_concurrent_manifest_creation_has_one_complete_winner(tmp_path: Path) -> None:
    target = tmp_path / "manifest.json"

    def write(run: int) -> str:
        try:
            write_json_exclusive(target, {"run": run, "payload": "x" * 1000})
            return "written"
        except FileExistsError:
            return "collision"

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(write, [1, 2]))
    assert sorted(results) == ["collision", "written"]
    assert json.loads(target.read_text(encoding="utf-8"))["run"] in {1, 2}
