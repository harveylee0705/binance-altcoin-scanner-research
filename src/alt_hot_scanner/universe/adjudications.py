from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from itertools import pairwise
from pathlib import Path
from typing import Any

from alt_hot_scanner.identity import require_semantic_contract_identity

ADJUDICATION_SCHEMA_VERSION = "lifecycle-adjudications-v1"
BOUNDARY_INDEX_SCHEMA_VERSION = "lifecycle-daily-trade-boundaries-v1"
EPISODE_FRESHNESS_SCHEMA_VERSION = "lifecycle-episode-freshness-v1"


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


def require_exclusive_utc_timestamp(value: object, field: str) -> str:
    """Require an explicit UTC timestamp while preserving its canonical spelling."""
    if type(value) is not str or not value.endswith("Z"):
        raise ValueError(f"{field} must be an explicit UTC timestamp ending in Z")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field} is not a valid UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError(f"{field} must be UTC")
    if parsed.isoformat().replace("+00:00", "Z") != value:
        raise ValueError(f"{field} is not in canonical UTC format")
    return value


def require_exclusive_utc_day_boundary(value: object, field: str) -> str:
    """Require the exact midnight UTC spelling used by the freshness CLI."""
    require_exclusive_utc_timestamp(value, field)
    if len(value) != 20 or value[-10:] != "T00:00:00Z":
        raise ValueError(f"{field} must be an exact UTC day boundary")
    return value


def _utc_day(value: str) -> date:
    parsed = datetime.fromisoformat(value)
    return parsed.astimezone(UTC).date() if parsed.tzinfo is not None else parsed.date()


def _episode_position(episode_id: object, symbol: str) -> int | None:
    prefix = f"{symbol}:"
    if type(episode_id) is not str or not episode_id.startswith(prefix):
        return None
    suffix = episode_id.removeprefix(prefix)
    return int(suffix) if suffix.isdigit() else None


def episode_freshness_review_required(
    daily_trade_rows: list[dict[str, Any]],
    lifecycle_adjudications: dict[str, Any],
    *,
    required_valid_through_utc: str,
    reviewed_delisting_records: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    """Detect machine-observable lifecycle changes without adjudicating them."""
    horizon = _utc_day(require_exclusive_utc_day_boundary(
        required_valid_through_utc, "required_valid_through_utc"
    ))
    by_symbol = lifecycle_adjudications.get("by_symbol", {})
    findings: list[dict[str, Any]] = []
    for row in sorted(daily_trade_rows, key=lambda item: item.get("symbol", "")):
        symbol = row.get("symbol")
        dates = row.get("observed_daily_trade_dates")
        if type(dates) is not list or dates != sorted(set(dates)):
            raise ValueError(f"Daily trade freshness dates are not canonical for {symbol}")
        dates_before_h = [item for item in dates if _utc_day(item) < horizon]
        adjudication = by_symbol.get(
            symbol,
            {"episodes": [{"episode_id": f"{symbol}:1"}]},
        )
        episodes = adjudication.get("episodes", [])
        gap = adjudication.get("gap_evidence") or {}
        reviewed_pairs = set()
        if gap.get("last_pre_gap_trade_archive_date") and gap.get(
            "first_post_gap_trade_archive_date"
        ):
            reviewed_pairs.add(
                (
                    gap["last_pre_gap_trade_archive_date"],
                    gap["first_post_gap_trade_archive_date"],
                )
            )
        observed_pairs = set()
        for previous, current in pairwise(dates_before_h):
            if _utc_day(current) - _utc_day(previous) > timedelta(days=1):
                observed_pairs.add((previous, current))
        unexplained = sorted(observed_pairs - reviewed_pairs)
        if unexplained:
            findings.append(_episode_finding(
                row, symbol, episodes, gap, unexplained[0],
                "new interior daily-trade discontinuity is not covered by a reviewed lifecycle boundary",
            ))
            continue

        if gap.get("last_pre_gap_trade_archive_date") and gap.get(
            "first_post_gap_trade_archive_date"
        ):
            pair = (
                gap["last_pre_gap_trade_archive_date"],
                gap["first_post_gap_trade_archive_date"],
            )
            if pair[0] in dates_before_h and pair[1] in dates_before_h:
                index = dates_before_h.index(pair[0])
                if index + 1 >= len(dates_before_h) or dates_before_h[index + 1] != pair[1]:
                    findings.append(_episode_finding(
                        row, symbol, episodes, gap, pair,
                        "reviewed lifecycle gap boundaries no longer agree with the complete daily-trade index",
                    ))
                    continue

        terminal_dates = []
        for episode in episodes:
            terminated_at = episode.get("terminated_at")
            if terminated_at:
                terminal_dates.append(_utc_day(terminated_at))
        if gap.get("last_terminal_trade_archive_date"):
            terminal_dates.append(_utc_day(gap["last_terminal_trade_archive_date"]))
        registry_terminal_for_latest = any(
            type(cutoff) is dict
            and cutoff.get("review_status") == "accepted_exact_cutoff"
            and cutoff.get("symbol") == symbol
            and cutoff.get("terminal_last_trading_at") is not None
            and _episode_position(cutoff.get("lifecycle_episode_id"), symbol)
            == len(episodes)
            for cutoff in reviewed_delisting_records or []
        )
        latest_is_open = (
            bool(episodes)
            and episodes[-1].get("terminated_at") is None
            and not registry_terminal_for_latest
        )
        if terminal_dates and not latest_is_open:
            terminal = max(terminal_dates)
            if any(_utc_day(item) > terminal for item in dates_before_h):
                findings.append(_episode_finding(
                    row, symbol, episodes, gap,
                    (str(terminal), dates_before_h[-1] if dates_before_h else None),
                    "daily trade archive appears after a reviewed terminal episode without a reviewed subsequent episode",
                ))
                continue
        for cutoff in reviewed_delisting_records or []:
            if (
                type(cutoff) is not dict
                or cutoff.get("review_status") != "accepted_exact_cutoff"
                or cutoff.get("symbol") != symbol
                or cutoff.get("terminal_last_trading_at") is None
            ):
                continue
            episode_position = _episode_position(
                cutoff.get("lifecycle_episode_id"), symbol
            )
            if episode_position is None:
                raise ValueError("Reviewed delisting cutoff has an invalid lifecycle episode")
            if any(
                (_episode_position(episode.get("episode_id"), symbol) or 0)
                > episode_position
                for episode in episodes
            ):
                continue
            terminal = _utc_day(cutoff["terminal_last_trading_at"])
            later_trade_dates = [
                item for item in dates_before_h if _utc_day(item) > terminal
            ]
            if later_trade_dates:
                findings.append(_episode_finding(
                    row, symbol, episodes, gap,
                    (cutoff["terminal_last_trading_at"], later_trade_dates[-1]),
                    "refreshed daily trade evidence appears after a reviewed registry terminal without a reviewed subsequent episode",
                ))
                break
        if latest_is_open:
            expected_frontier_date = (horizon - timedelta(days=1)).isoformat()
            if expected_frontier_date not in dates_before_h:
                findings.append(_episode_finding(
                    row, symbol, episodes, gap,
                    (dates_before_h[-1] if dates_before_h else None, expected_frontier_date),
                    "reviewed open/latest episode lacks expected frontier daily-trade evidence up to the requested horizon",
                ))
    if not findings:
        return None
    return {
        "schema_version": "lifecycle-episode-freshness-review-required-v1",
        "status": "review_required",
        "required_valid_through_utc": required_valid_through_utc,
        "findings": findings,
        "message": "Machine evidence detected a possible lifecycle gap or resumption; reviewed adjudication is required and no episode was changed.",
    }


def _episode_finding(
    row: dict[str, Any],
    symbol: str,
    episodes: list[dict[str, Any]],
    gap: dict[str, Any],
    detected: Any,
    reason: str,
) -> dict[str, Any]:
    paths = row.get("raw_snapshot_paths") or []
    hashes = row.get("raw_snapshot_sha256s") or []
    return {
        "symbol": symbol,
        "observed_daily_trade_dates": row.get("observed_daily_trade_dates", []),
        "relevant_existing_episode_ids": [episode.get("episode_id") for episode in episodes],
        "existing_reviewed_boundaries": gap,
        "detected_candidate_gap_or_resumption": detected,
        "raw_evidence_paths": paths,
        "raw_evidence_sha256s": hashes,
        "reason": reason,
    }
