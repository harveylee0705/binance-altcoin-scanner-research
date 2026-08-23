from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from alt_hot_scanner.data.binance_public import (
    observed_zip_keys_from_index_snapshots,
    sha256_file,
    validate_archive_object_key,
)
from alt_hot_scanner.data.provenance import load_snapshot_provenance
from alt_hot_scanner.identity import require_binance_token, require_semantic_contract_identity

ARCHIVE_CHECKPOINT_SCHEMA_VERSION = "archive-lifecycle-checkpoint-v2"


def _confined_snapshot(path_text: object, raw_run_root: Path) -> Path:
    if type(path_text) is not str or not path_text:
        raise ValueError("Checkpoint raw snapshot path must be nonempty text")
    path = Path(path_text)
    if not path.is_absolute():
        raise ValueError("Checkpoint raw snapshot paths must be absolute")
    resolved = path.resolve(strict=True)
    expected_root = (raw_run_root / "archive_index").resolve(strict=True)
    if not resolved.is_relative_to(expected_root):
        raise ValueError("Checkpoint raw XML path escapes the expected raw run directory")
    return resolved


def _verify_snapshot(
    path_text: object, expected_sha256: object, raw_run_root: Path
) -> tuple[Path, str | None]:
    path = _confined_snapshot(path_text, raw_run_root)
    computed, _ = sha256_file(path)
    if expected_sha256 != computed:
        raise ValueError("Checkpoint raw XML hash mismatch")
    sidecar = path.with_name(f"{path.name}.provenance.json")
    retrieval: str | None = None
    if sidecar.exists():
        provenance = json.loads(sidecar.read_text(encoding="utf-8"))
        url = provenance.get("url")
        if type(url) is not str or not url:
            raise ValueError("Checkpoint provenance URL is malformed")
        verified = load_snapshot_provenance(path, expected_url=url, expected_sha256=computed)
        retrieval = verified["original_retrieval_timestamp"] if verified else None
    return path, retrieval


def _symbol_prefixes(paths: list[Path]) -> tuple[list[str], list[str]]:
    namespace = "http://s3.amazonaws.com/doc/2006-03-01/"
    prefix_root = "data/futures/um/monthly/klines/"
    canonical: set[str] = set()
    noncanonical: set[str] = set()
    for path in paths:
        try:
            root = ET.fromstring(path.read_bytes())
        except ET.ParseError as exc:
            raise ValueError("Checkpoint symbol XML is malformed") from exc
        if root.tag != f"{{{namespace}}}ListBucketResult":
            raise ValueError("Checkpoint symbol XML namespace is invalid")
        for node in root.findall(
            f"{{{namespace}}}CommonPrefixes/{{{namespace}}}Prefix"
        ):
            value = node.text
            if not value or not value.startswith(prefix_root) or not value.endswith("/"):
                raise ValueError("Checkpoint symbol XML contains a malformed prefix")
            identity = require_semantic_contract_identity(
                value[len(prefix_root) : -1], "archive semantic identity"
            )
            try:
                canonical.add(require_binance_token(identity, "archive symbol"))
            except ValueError:
                noncanonical.add(identity)
    return sorted(canonical), sorted(noncanonical)


def verify_archive_checkpoint(
    checkpoint_path: str | Path, raw_run_root: str | Path
) -> dict[str, Any]:
    """Reconstruct primitive archive state; checkpoint-derived observations are never trusted."""
    path = Path(checkpoint_path).resolve(strict=True)
    root = Path(raw_run_root).resolve(strict=True)
    if path.parent != root:
        raise ValueError("Archive checkpoint is outside the expected raw run root")
    checkpoint = json.loads(path.read_text(encoding="utf-8"))
    required = {
        "schema_version",
        "canonical_symbols",
        "quarantined_prefixes",
        "symbol_audit",
        "symbol_raw_snapshot_paths",
        "symbol_raw_snapshot_sha256s",
        "archive_observations",
    }
    if type(checkpoint) is not dict or set(checkpoint) != required:
        raise ValueError("Archive checkpoint fields do not match the exact schema")
    if checkpoint["schema_version"] != ARCHIVE_CHECKPOINT_SCHEMA_VERSION:
        raise ValueError("Archive checkpoint has an unsupported schema")
    symbol_paths = checkpoint["symbol_raw_snapshot_paths"]
    symbol_hashes = checkpoint["symbol_raw_snapshot_sha256s"]
    if type(symbol_paths) is not list or type(symbol_hashes) is not list or len(
        symbol_paths
    ) != len(symbol_hashes):
        raise ValueError("Archive checkpoint symbol snapshot lineage is malformed")
    verified_symbol_paths = [
        _verify_snapshot(raw_path, digest, root)[0]
        for raw_path, digest in zip(symbol_paths, symbol_hashes, strict=True)
    ]
    canonical, noncanonical = _symbol_prefixes(verified_symbol_paths)
    if canonical != checkpoint["canonical_symbols"] or noncanonical != checkpoint[
        "quarantined_prefixes"
    ]:
        raise ValueError("Checkpoint symbol identities disagree with raw XML")
    observations = checkpoint["archive_observations"]
    if type(observations) is not list or len(observations) != len(canonical):
        raise ValueError("Checkpoint archive observations are incomplete")
    reconstructed: list[dict[str, Any]] = []
    for row in observations:
        if type(row) is not dict:
            raise ValueError("Checkpoint observation must be an object")
        symbol = require_binance_token(row.get("symbol"), "checkpoint symbol")
        raw_paths = row.get("archive_raw_snapshot_paths")
        raw_hashes = row.get("archive_raw_snapshot_sha256s")
        if type(raw_paths) is not list or type(raw_hashes) is not list or len(raw_paths) != len(
            raw_hashes
        ):
            raise ValueError("Checkpoint observation raw lineage is malformed")
        verified: list[str] = []
        retrievals: list[str | None] = []
        for raw_path, digest in zip(raw_paths, raw_hashes, strict=True):
            verified_path, retrieval = _verify_snapshot(raw_path, digest, root)
            verified.append(str(verified_path))
            retrievals.append(retrieval)
        keys = list(observed_zip_keys_from_index_snapshots(verified, symbol))
        periods = sorted(validate_archive_object_key(key).period for key in keys)
        primitive = {
            **row,
            "first_archive_month": periods[0],
            "last_archive_month": periods[-1],
            "observed_archive_object_keys": keys,
            "unique_archive_count": len(keys),
            "archive_raw_snapshot_paths": verified,
            "archive_raw_snapshot_sha256s": raw_hashes,
            "archive_discovery_timestamp": (
                min(value for value in retrievals if value is not None)
                if any(value is not None for value in retrievals)
                else None
            ),
        }
        for field in (
            "first_archive_month",
            "last_archive_month",
            "observed_archive_object_keys",
            "unique_archive_count",
        ):
            if row.get(field) != primitive[field]:
                raise ValueError(f"Checkpoint derived field disagrees with raw XML: {field}")
        reconstructed.append(primitive)
    if sorted(row["symbol"] for row in reconstructed) != canonical:
        raise ValueError("Checkpoint observation identities disagree with symbol XML")
    return {
        "canonical_symbols": canonical,
        "quarantined_prefixes": noncanonical,
        "symbol_audit": checkpoint["symbol_audit"],
        "archive_observations": reconstructed,
    }
