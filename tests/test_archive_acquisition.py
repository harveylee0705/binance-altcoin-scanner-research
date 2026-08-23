from __future__ import annotations

import hashlib
import inspect
import urllib.error

import pytest

import alt_hot_scanner.data.binance_public as public_data
from alt_hot_scanner.data.binance_public import (
    ArchiveAcquisitionError,
    download_verified_archive,
)
from scripts.download_from_plan import failure_attempt

KEY = "data/futures/um/monthly/klines/ETHUSDT/1h/ETHUSDT-1h-2023-06.zip"


def _http_404(url: str) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(url, 404, "Not Found", hdrs=None, fp=None)


def test_archive_404_is_missing_but_checksum_404_for_existing_archive_is_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.setattr(public_data, "_read_url", lambda url: (_ for _ in ()).throw(_http_404(url)))
    monkeypatch.setattr(public_data, "object_exists", lambda key: False)
    with pytest.raises(ArchiveAcquisitionError) as missing_error:
        download_verified_archive(KEY, tmp_path)
    assert missing_error.value.stage == "archive_payload"
    assert failure_attempt(KEY, missing_error.value)["status"] == "missing"

    monkeypatch.setattr(public_data, "object_exists", lambda key: True)
    with pytest.raises(ArchiveAcquisitionError) as checksum_error:
        download_verified_archive(KEY, tmp_path)
    attempt = failure_attempt(KEY, checksum_error.value)
    assert checksum_error.value.stage == "checksum_sidecar"
    assert attempt["status"] == "failed"
    assert attempt["failure_url"].endswith(".CHECKSUM")


def test_checksum_mismatch_preserves_existing_raw_bytes_and_hash_evidence(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    old_payload = b"immutable-original"
    new_payload = b"republished-different"
    published = hashlib.sha256(new_payload).hexdigest()
    destination = tmp_path / KEY
    destination.parent.mkdir(parents=True)
    destination.write_bytes(old_payload)
    monkeypatch.setattr(
        public_data,
        "_read_url",
        lambda url: f"{published}  {destination.name}\n".encode(),
    )

    with pytest.raises(ArchiveAcquisitionError) as mismatch:
        download_verified_archive(KEY, tmp_path)
    attempt = failure_attempt(KEY, mismatch.value)
    assert mismatch.value.stage == "local_corruption"
    assert destination.read_bytes() == old_payload
    assert attempt["published_sha256"] == published
    assert attempt["computed_sha256"] == hashlib.sha256(old_payload).hexdigest()
    assert attempt["status"] == "failed"
    assert "overwrite" not in inspect.signature(download_verified_archive).parameters


def test_downloaded_and_cached_success_are_both_verified(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    payload = b"valid-archive-payload"
    published = hashlib.sha256(payload).hexdigest()

    def fake_read(url: str) -> bytes:
        if url.endswith(".CHECKSUM"):
            return f"{published}  {KEY.rsplit('/', 1)[-1]}\n".encode()
        return payload

    monkeypatch.setattr(public_data, "_read_url", fake_read)
    downloaded = download_verified_archive(KEY, tmp_path)
    cached = download_verified_archive(KEY, tmp_path)
    assert downloaded.payload_source == "downloaded_verified_then_atomic_no_replace_install"
    assert cached.payload_source == "existing_local_verified_against_published_checksum"
    assert downloaded.published_sha256 == downloaded.computed_sha256 == published
    assert (tmp_path / KEY).read_bytes() == payload
