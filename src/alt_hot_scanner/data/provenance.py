from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from alt_hot_scanner.data.binance_public import write_json_exclusive

PROVENANCE_SCHEMA_VERSION = "raw-source-provenance-v1"


def provenance_path(snapshot_path: str | Path) -> Path:
    path = Path(snapshot_path)
    return path.with_name(f"{path.name}.provenance.json")


def record_new_snapshot_provenance(
    snapshot_path: str | Path,
    *,
    url: str,
    parser_version: str,
    retrieved_at: str | None = None,
) -> dict[str, Any]:
    """Write an immutable sidecar for bytes acquired during this request."""
    path = Path(snapshot_path)
    payload = {
        "schema_version": PROVENANCE_SCHEMA_VERSION,
        "url": url,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "local_snapshot_path": str(path.resolve()),
        "original_retrieval_timestamp": retrieved_at or datetime.now(UTC).isoformat(),
        "acquisition_parser_version": parser_version,
    }
    write_json_exclusive(provenance_path(path), payload)
    return payload


def load_snapshot_provenance(
    snapshot_path: str | Path,
    *,
    expected_url: str | None = None,
    expected_sha256: str,
) -> dict[str, Any] | None:
    """Load and verify an immutable sidecar; legacy bytes without one stay unresolved."""
    path = provenance_path(snapshot_path)
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != PROVENANCE_SCHEMA_VERSION:
        raise ValueError("Raw source provenance has an unsupported schema")
    if (expected_url is not None and payload.get("url") != expected_url) or payload.get(
        "sha256"
    ) != expected_sha256:
        raise ValueError("Raw source provenance does not match the cached snapshot")
    if payload.get("local_snapshot_path") != str(Path(snapshot_path).resolve()):
        raise ValueError("Raw source provenance path does not match the cached snapshot")
    timestamp = payload.get("original_retrieval_timestamp")
    if not isinstance(timestamp, str) or not timestamp:
        raise ValueError("Raw source provenance lacks its original retrieval timestamp")
    return payload
