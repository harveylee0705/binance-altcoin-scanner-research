from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DELISTING_REGISTRY_SCHEMA_VERSION = "historical-delisting-cutoff-registry-v1"
DELISTING_REVIEW_SCHEMA_VERSION = "historical-delisting-cutoff-review-v1"
_SHA256 = re.compile(r"[0-9a-f]{64}")


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def _identity(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def cms_corpus_binding(audit: dict[str, Any]) -> dict[str, Any]:
    catalogs = [
        item for item in audit.get("catalogs", []) if item.get("event_type") == "delisting"
    ]
    if len(catalogs) != 1:
        raise ValueError("Announcement audit lacks one exact delisting catalog")
    catalog = catalogs[0]
    stable = {
        "schema_version": "binance-delisting-cms-corpus-v1",
        "parser_version": audit.get("parser_version"),
        "catalog_id": catalog.get("catalog_id"),
        "declared_total": catalog.get("declared_total"),
        "pages": catalog.get("pages"),
        "candidate_articles": catalog.get("candidate_articles"),
        "inspection_policy": catalog.get("inspection_policy"),
        "page_sha256s": catalog.get("page_sha256s"),
        "detail_sha256s": sorted(
            catalog.get("detail_sha256s", []), key=lambda item: item.get("article_code", "")
        ),
    }
    digest = _identity(stable)
    return {"identity": digest, "sha256": digest, "delisting_catalog_id": catalog["catalog_id"]}


def verify_cms_corpus_binding(registry: dict[str, Any], audit: dict[str, Any]) -> None:
    if registry.get("official_cms_corpus") != cms_corpus_binding(audit):
        raise ValueError("Delisting registry targets a stale or wrong official CMS corpus")


def _utc_instant(value: Any, field: str) -> datetime:
    if type(value) is not str:
        raise ValueError(f"Delisting {field} is not an exact UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Delisting {field} is not an exact UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"Delisting {field} is not an exact UTC timestamp")
    return parsed.astimezone(UTC)


def verify_delisting_records_against_announcement_evidence(
    registry: dict[str, Any], announcement_evidence: list[dict[str, Any]]
) -> None:
    """Bind accepted reviewed delisting records to independently derived CMS rows."""
    records = registry.get("records")
    if type(records) is not list or type(announcement_evidence) is not list:
        raise ValueError("Delisting registry or announcement evidence is malformed")
    for record in records:
        if type(record) is not dict:
            raise ValueError("Delisting registry record is malformed")
        if record.get("review_status") != "accepted_exact_cutoff":
            continue
        if (
            type(record.get("symbol")) is not str
            or type(record.get("article_code")) is not str
            or type(record.get("official_article_url")) is not str
            or _SHA256.fullmatch(str(record.get("raw_article_sha256"))) is None
        ):
            raise ValueError("Accepted delisting cutoff lacks exact official evidence")
        if record.get("product_event_disposition") != "binance_usdm_futures_termination":
            raise ValueError("Accepted delisting cutoff has the wrong product disposition")

        symbol = record.get("symbol")
        applicable = [
            row
            for row in announcement_evidence
            if type(row) is dict
            and row.get("event_type") == "delisting"
            and row.get("symbol") == symbol
            and row.get("article_code") == record.get("article_code")
        ]
        if len(applicable) != 1:
            raise ValueError(
                "Accepted delisting cutoff does not have exactly one applicable replayed evidence row"
            )
        evidence = applicable[0]
        exact_fields = (
            ("article_code", "article_code"),
            ("raw_article_sha256", "raw_snapshot_sha256"),
            ("official_article_url", "source_url"),
        )
        if any(
            record.get(registry_field) != evidence.get(evidence_field)
            for registry_field, evidence_field in exact_fields
        ):
            raise ValueError("Accepted delisting cutoff does not bind the replayed article evidence")
        if (
            evidence.get("match_status") != "accepted"
            or evidence.get("article_semantic_class") != "delisting_or_settlement"
            or evidence.get("semantic_evidence_status")
            != "accepted_positive_semantic_evidence"
        ):
            raise ValueError("Accepted delisting cutoff is supported by non-accepted semantic evidence")
        if _utc_instant(
            record.get("official_publication_timestamp"), "publication timestamp"
        ) != _utc_instant(evidence.get("article_published_at"), "article publication timestamp"):
            raise ValueError("Accepted delisting cutoff publication timestamp does not match replayed evidence")

        terminal = record.get("terminal_last_trading_at")
        official_event_at = evidence.get("official_event_at")
        if terminal is None:
            if official_event_at is not None:
                raise ValueError(
                    "Accepted delisting cutoff omits a replayed terminal event time"
                )
            if evidence.get("event_time_evidence_status") == "action_symbol_time_anchored":
                raise ValueError(
                    "Replayed terminal event evidence is internally contradictory"
                )
        else:
            if (
                evidence.get("event_time_evidence_status") != "action_symbol_time_anchored"
                or official_event_at is None
            ):
                raise ValueError(
                    "Accepted delisting cutoff lacks an anchored replayed terminal event time"
                )
            if _utc_instant(terminal, "terminal last-trading timestamp") != _utc_instant(
                official_event_at, "official event timestamp"
            ):
                raise ValueError(
                    "Accepted delisting cutoff terminal timestamp does not match replayed evidence"
                )


def load_delisting_registry(
    path: str | Path,
    *,
    candidate_set_digest: str,
    review_path: str | Path | None = None,
) -> dict[str, Any]:
    source = Path(path).resolve(strict=True)
    payload = json.loads(source.read_text("utf-8"))
    if payload.get("schema_version") != DELISTING_REGISTRY_SCHEMA_VERSION:
        raise ValueError("Delisting registry has an unsupported schema")
    core = {key: value for key, value in payload.items() if key != "registry_id"}
    if payload.get("registry_id") != _identity(core):
        raise ValueError("Delisting registry identity is invalid")
    if payload.get("candidate_set_digest") != candidate_set_digest:
        raise ValueError("Delisting registry targets the wrong candidate set")
    records = payload.get("records")
    if type(records) is not list:
        raise ValueError("Delisting registry records must be a list")
    by_episode: dict[str, dict[str, Any]] = {}
    for record in records:
        if type(record) is not dict:
            raise ValueError("Delisting registry record must be an object")
        episode_id = record.get("lifecycle_episode_id")
        symbol = record.get("symbol")
        if type(episode_id) is not str or type(symbol) is not str or not episode_id.startswith(
            f"{symbol}:"
        ):
            raise ValueError("Delisting registry episode identity is invalid")
        if episode_id in by_episode:
            raise ValueError("Delisting registry contains duplicate episodes")
        status = record.get("review_status")
        cutoff = record.get("official_publication_timestamp")
        if status == "accepted_exact_cutoff":
            required = ("article_code", "official_article_url", "raw_article_sha256", cutoff)
            if any(value is None for value in required) or _SHA256.fullmatch(
                str(record.get("raw_article_sha256"))
            ) is None:
                raise ValueError("Accepted delisting cutoff lacks exact official evidence")
            if record.get("product_event_disposition") != "binance_usdm_futures_termination":
                raise ValueError("Accepted delisting cutoff has the wrong product disposition")
        elif status not in {
            "reviewed_no_reliable_cutoff",
            "not_applicable_current_episode",
            "unresolved_conflicting_evidence",
        } or cutoff is not None:
            raise ValueError("Delisting registry disposition is invalid")
        by_episode[episode_id] = record
    review = None
    if review_path is not None:
        review_source = Path(review_path).resolve(strict=True)
        review = json.loads(review_source.read_text("utf-8"))
        review_core = {key: value for key, value in review.items() if key != "review_id"}
        if (
            review.get("schema_version") != DELISTING_REVIEW_SCHEMA_VERSION
            or review.get("review_id") != _identity(review_core)
            or review.get("verdict") != "PASS"
            or review.get("registry_id") != payload["registry_id"]
            or review.get("registry_sha256")
            != hashlib.sha256(source.read_bytes()).hexdigest()
            or review.get("reviewed_episode_count") != len(records)
        ):
            raise ValueError("Delisting registry independent review is invalid")
    return {**payload, "path": source, "by_episode": by_episode, "review": review}
