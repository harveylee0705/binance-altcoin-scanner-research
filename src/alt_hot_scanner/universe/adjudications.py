from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from alt_hot_scanner.identity import require_semantic_contract_identity

ADJUDICATION_SCHEMA_VERSION = "lifecycle-adjudications-v1"
BOUNDARY_INDEX_SCHEMA_VERSION = "lifecycle-daily-trade-boundaries-v1"


def _identity(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(payload).hexdigest()


def load_lifecycle_adjudications(
    path: str | Path, *, candidate_set_digest: str
) -> dict[str, Any]:
    source = Path(path).resolve(strict=True)
    payload = json.loads(source.read_text(encoding="utf-8"))
    if payload.get("schema_version") != ADJUDICATION_SCHEMA_VERSION:
        raise ValueError("Lifecycle adjudications have an unsupported schema")
    core = {key: value for key, value in payload.items() if key != "adjudication_id"}
    if payload.get("adjudication_id") != _identity(core):
        raise ValueError("Lifecycle adjudication identity is invalid")
    if payload.get("candidate_set_digest") != candidate_set_digest:
        raise ValueError("Lifecycle adjudications target the wrong candidate set")
    records = payload.get("records")
    if type(records) is not list:
        raise ValueError("Lifecycle adjudication records must be a list")
    by_symbol: dict[str, dict[str, Any]] = {}
    for position, record in enumerate(records):
        if type(record) is not dict:
            raise ValueError("Lifecycle adjudication record must be an object")
        symbol = require_semantic_contract_identity(
            record.get("symbol"), f"adjudication.records[{position}].symbol"
        )
        if symbol in by_symbol:
            raise ValueError("Lifecycle adjudications contain duplicate symbols")
        episodes = record.get("episodes")
        if type(episodes) is not list or not episodes:
            raise ValueError("Each lifecycle adjudication requires at least one episode")
        expected_ids = [f"{symbol}:{index}" for index in range(1, len(episodes) + 1)]
        if [episode.get("episode_id") for episode in episodes] != expected_ids:
            raise ValueError("Lifecycle episode identifiers must be ordered and canonical")
        by_symbol[symbol] = record
    return {**payload, "path": source, "by_symbol": by_symbol}


def verify_boundary_index(
    adjudications: dict[str, Any], boundary_rows: list[dict[str, Any]]
) -> None:
    by_symbol = {row.get("symbol"): row for row in boundary_rows}
    if len(by_symbol) != len(boundary_rows):
        raise ValueError("Daily trade boundary evidence contains duplicate symbols")
    for symbol, record in adjudications["by_symbol"].items():
        gap = record.get("gap_evidence")
        if not gap:
            continue
        boundary = by_symbol.get(symbol)
        if boundary is None or boundary.get("schema_version") != BOUNDARY_INDEX_SCHEMA_VERSION:
            raise ValueError(f"Missing daily trade boundary evidence for {symbol}")
        observed = boundary.get("observed_archive_dates")
        if type(observed) is not list or observed != sorted(set(observed)):
            raise ValueError("Daily trade boundary dates are not canonical")
        last_pre = gap.get("last_pre_gap_trade_archive_date")
        first_post = gap.get("first_post_gap_trade_archive_date")
        terminal = gap.get("last_terminal_trade_archive_date")
        if last_pre is not None:
            if last_pre not in observed or first_post not in observed:
                raise ValueError(f"Reviewed trade gap boundary is absent for {symbol}")
            left = observed.index(last_pre)
            if left + 1 >= len(observed) or observed[left + 1] != first_post:
                raise ValueError(f"Unreviewed daily trade date appears inside the gap for {symbol}")
        if terminal is not None and observed[-1] != terminal:
            raise ValueError(f"Terminal daily trade boundary changed for {symbol}")
