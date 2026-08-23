from __future__ import annotations

import hashlib
import html
import json
import re
import time
import unicodedata
import urllib.error
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import pandas as pd

from alt_hot_scanner.data.binance_public import _read_url, _write_bytes_exclusive_atomic
from alt_hot_scanner.data.provenance import (
    load_snapshot_provenance,
    record_new_snapshot_provenance,
)
from alt_hot_scanner.identity import require_binance_token, require_canonical_text
from alt_hot_scanner.utils.numeric import strict_millisecond_timestamp

ANNOUNCEMENT_API = "https://www.binance.com/bapi/composite/v1/public/cms/article"
ANNOUNCEMENT_PAGE = "https://www.binance.com/en/support/announcement/detail"
ANNOUNCEMENT_PARSER_VERSION = "binance-announcement-semantic-v3"
LISTING_CATALOG_ID = 48
DELISTING_CATALOG_ID = 161
PAGE_SIZE = 50

_DIRECT_SYMBOL = re.compile(
    r"(?<![A-Z0-9])([A-Z0-9]{2,60}(?:USDT|BUSD|USDC))(?![A-Z0-9])"
)
_SLASH_SYMBOL = re.compile(r"(?<![A-Z0-9])([A-Z0-9]{1,60})\s*/\s*USDT(?![A-Z0-9])")
_DATE_FIRST = re.compile(
    r"(?P<date>20\d{2}[-/]\d{2}[-/]\d{2})\s+(?:at\s+)?"
    r"(?P<time>\d{1,2}:\d{2}(?::\d{2})?)\s*(?P<ampm>AM|PM)?\s*\(UTC\)",
    re.IGNORECASE,
)
_TIME_FIRST = re.compile(
    r"(?P<time>\d{1,2}:\d{2}(?::\d{2})?)\s*(?P<ampm>AM|PM)?\s*\(UTC\)"
    r"\s*(?:on\s+)(?P<date>20\d{2}[-/]\d{2}[-/]\d{2})",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class AnnouncementEvidence:
    event_type: str
    symbol: str
    match_status: str
    match_basis: str
    article_semantic_class: str
    semantic_evidence_status: str
    article_code: str
    article_title: str
    article_published_at: str
    official_event_at: str | None
    event_time_evidence_status: str
    source_url: str
    retrieved_at: str | None
    raw_snapshot_path: str
    raw_snapshot_sha256: str
    parser_version: str


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        if data.strip():
            self.parts.append(data.strip())


def _require_official_prose(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be nonempty text")
    if unicodedata.normalize("NFC", value) != value:
        raise ValueError(f"{field} must use canonical Unicode normalization")
    if any(character == "\x00" for character in value):
        raise ValueError(f"{field} contains a null character")
    return value


def _structured_text(node: object, output: list[str]) -> None:
    if isinstance(node, dict):
        text_value = node.get("text")
        if isinstance(text_value, str) and text_value.strip():
            output.append(text_value.strip())
        for child in node.get("child", []):
            _structured_text(child, output)
    elif isinstance(node, list):
        for child in node:
            _structured_text(child, output)


def article_plain_text(body: object) -> str:
    body_text = _require_official_prose(body, "article body")
    try:
        parsed = json.loads(body_text)
    except json.JSONDecodeError:
        extractor = _TextExtractor()
        extractor.feed(body_text)
        parts = extractor.parts
    else:
        parts: list[str] = []
        _structured_text(parsed, parts)
    normalized = [re.sub(r"\s+", " ", html.unescape(part)).strip() for part in parts]
    return "\n".join(part for part in normalized if part)


def _symbols_in_order(text: str, archive_symbols: set[str]) -> list[str]:
    matches: list[tuple[int, str]] = []
    for match in _DIRECT_SYMBOL.finditer(text):
        matches.append((match.start(), match.group(1)))
    for match in _SLASH_SYMBOL.finditer(text):
        matches.append((match.start(), f"{match.group(1)}USDT"))
    ordered: list[str] = []
    for _, symbol in sorted(matches):
        if symbol in archive_symbols and symbol not in ordered:
            ordered.append(require_binance_token(symbol, "announcement symbol"))
    return ordered


def _parse_timestamp(match: re.Match[str]) -> pd.Timestamp:
    date = match.group("date").replace("/", "-")
    clock = match.group("time")
    ampm = match.group("ampm")
    value = f"{date} {clock} {ampm or ''}".strip()
    if ampm:
        clock_format = "%I:%M:%S" if clock.count(":") == 2 else "%I:%M"
        parsed = pd.Timestamp(
            datetime.strptime(value, f"%Y-%m-%d {clock_format} %p").replace(tzinfo=UTC)
        )
    else:
        parsed = pd.Timestamp(value, tz="UTC")
    return parsed


_LAUNCH_ACTION = re.compile(
    r"\b(?:will\s+)?(?:launch(?:es)?|list(?:s)?|introduce(?:s)?|begin\s+trading|"
    r"trading\s+(?:will\s+)?start)\b",
    re.IGNORECASE,
)
_DELIST_ACTION = re.compile(
    r"\b(?:will\s+)?(?:delist|cease\s+trading|settle|close\s+all\s+positions|"
    r"terminate(?:s|d)?\s+(?:the\s+)?contract)\b",
    re.IGNORECASE,
)


def classify_article_semantics(title: str, body_text: str) -> str:
    """Classify product intent before any exact symbol can become event evidence."""
    combined = f"{title}\n{body_text[:4000]}".lower()
    title_lower = title.lower()
    if "copy trading" in title_lower:
        return "copy_trading_enablement"
    if "trading bot" in title_lower or "futures grid" in title_lower:
        return "trading_bot_enablement"
    if any(
        term in title_lower for term in ("portfolio margin", "multi-assets mode", "multi-assets")
    ):
        return "portfolio_margin_or_multi_asset_enablement"
    if "pre-market" in title_lower or "pre market" in title_lower:
        return "pre_market_or_other_product_enablement"
    title_futures_product = "binance futures" in title_lower or "usdⓢ-m futures" in title_lower
    title_perpetual_product = "perpetual" in title_lower and "contract" in title_lower
    if title_futures_product and title_perpetual_product and _DELIST_ACTION.search(title_lower):
        return "delisting_or_settlement"
    if any(
        term in title_lower
        for term in (
            "leverage and margin tiers",
            "leverage & margin tiers",
            "funding rate settlement frequency",
            "tick size",
            "contract specifications",
            "parameter update",
        )
    ):
        return "contract_parameter_update"
    if any(
        term in title_lower for term in ("maintenance", "system upgrade", "temporary suspension")
    ):
        return "maintenance_or_operational"
    if title_futures_product and title_perpetual_product and _LAUNCH_ACTION.search(title_lower):
        return "original_perpetual_launch"
    if "copy trading" in combined:
        return "copy_trading_enablement"
    if "trading bot" in combined or "futures grid" in combined:
        return "trading_bot_enablement"
    futures_product = "binance futures" in combined or "usdⓢ-m futures" in combined
    perpetual_product = "perpetual" in combined and "contract" in combined
    if futures_product and perpetual_product and _DELIST_ACTION.search(combined):
        return "delisting_or_settlement"
    if futures_product and perpetual_product and _LAUNCH_ACTION.search(combined):
        return "original_perpetual_launch"
    if "futures" in combined or "perpetual" in combined or "contract" in combined:
        return "ambiguous"
    return "irrelevant"


def _segments(text: str) -> list[str]:
    segments: list[str] = []
    for line in text.splitlines():
        for segment in re.split(r"(?<=[.!?;])\s+", line):
            if segment.strip():
                segments.append(segment.strip())
    return segments


def _timestamps_in_segment(segment: str) -> list[pd.Timestamp]:
    matches = [*_DATE_FIRST.finditer(segment), *_TIME_FIRST.finditer(segment)]
    parsed: list[tuple[int, pd.Timestamp]] = []
    for match in matches:
        timestamp = _parse_timestamp(match)
        if timestamp not in [item[1] for item in parsed]:
            parsed.append((match.start(), timestamp))
    return [timestamp for _, timestamp in sorted(parsed)]


def _action_anchored_event_times(
    text: str,
    symbols: list[str],
    semantic_class: str,
    archive_symbols: set[str],
) -> dict[str, pd.Timestamp | None]:
    """Map times only inside an explicit action/symbol structure; never by position/count."""
    action = _LAUNCH_ACTION if semantic_class == "original_perpetual_launch" else _DELIST_ACTION
    mapped: dict[str, pd.Timestamp | None] = {symbol: None for symbol in symbols}
    candidates: dict[str, list[pd.Timestamp]] = {symbol: [] for symbol in symbols}
    segments = _segments(text[:8000])
    action_context = False
    for segment in segments:
        segment_symbols = [
            symbol for symbol in _symbols_in_order(segment, archive_symbols) if symbol in mapped
        ]
        times = _timestamps_in_segment(segment)
        has_action = action.search(segment) is not None
        residual = segment
        for pattern in (_DATE_FIRST, _TIME_FIRST):
            residual = pattern.sub(" ", residual)
        for symbol in segment_symbols:
            residual = residual.replace(symbol, " ")
        residual = re.sub(r"\b(?:and|or|UTC)\b|[\s:;,()\-/]", "", residual, flags=re.IGNORECASE)
        # A carried launch header is allowed only for a bare symbol/time table row.
        # Any product, operational, or enablement prose terminates the context.
        structured_row = (
            action_context
            and bool(segment_symbols)
            and len(times) == 1
            and residual == ""
        )
        explicit_shared = has_action and bool(segment_symbols) and len(times) == 1
        if structured_row or explicit_shared:
            for symbol in segment_symbols:
                candidates[symbol].append(times[0])
        if has_action and not segment_symbols and not times:
            action_context = True
        elif not structured_row:
            action_context = False
    for symbol, values in candidates.items():
        unique = list(dict.fromkeys(values))
        if len(unique) == 1:
            mapped[symbol] = unique[0]
    return mapped


def _candidate_title(event_type: str, title: str) -> bool:
    normalized = title.lower()
    if "binance futures" not in normalized:
        return False
    if event_type == "listing":
        if "delivery" in normalized or "quarterly" in normalized:
            return False
        return ("launch" in normalized or "adds" in normalized) and (
            "perpetual" in normalized or "contract" in normalized
        )
    return "delist" in normalized and ("perpetual" in normalized or "contract" in normalized)


def parse_announcement_evidence(
    detail_payload: dict[str, Any],
    index_record: dict[str, Any],
    event_type: str,
    archive_symbols: set[str],
    *,
    retrieved_at: str | None,
    raw_snapshot_path: str,
    raw_snapshot_sha256: str,
) -> list[AnnouncementEvidence]:
    data = detail_payload.get("data")
    if detail_payload.get("code") != "000000" or not isinstance(data, dict):
        raise ValueError("Official announcement detail response is malformed")
    title = _require_official_prose(data.get("title"), "article title")
    body_text = article_plain_text(data.get("body"))
    title_symbols = _symbols_in_order(title, archive_symbols)
    symbols = title_symbols or _symbols_in_order(body_text, archive_symbols)
    if not symbols:
        return []
    release_ms = strict_millisecond_timestamp(index_record.get("releaseDate"), "releaseDate")
    published = pd.to_datetime(release_ms, unit="ms", utc=True)
    semantic_class = classify_article_semantics(title, body_text)
    expected_class = (
        "original_perpetual_launch" if event_type == "listing" else "delisting_or_settlement"
    )
    semantic_accepted = semantic_class == expected_class
    mapped = (
        _action_anchored_event_times(body_text, symbols, semantic_class, archive_symbols)
        if semantic_accepted
        else {symbol: None for symbol in symbols}
    )

    code = require_canonical_text(index_record.get("code"), "article code")
    return [
        AnnouncementEvidence(
            event_type=event_type,
            symbol=symbol,
            match_status=("accepted" if semantic_accepted else "rejected_semantic_class"),
            match_basis=(
                "positive_product_semantics_and_exact_symbol"
                if semantic_accepted
                else "exact_symbol_but_non_original_or_non_delisting_semantics"
            ),
            article_semantic_class=semantic_class,
            semantic_evidence_status=(
                "accepted_positive_semantic_evidence"
                if semantic_accepted
                else "rejected_not_applicable_event_semantics"
            ),
            article_code=code,
            article_title=title,
            article_published_at=published.isoformat(),
            official_event_at=mapped[symbol].isoformat() if mapped[symbol] is not None else None,
            event_time_evidence_status=(
                "action_symbol_time_anchored"
                if mapped[symbol] is not None
                else "unresolved"
            ),
            source_url=f"{ANNOUNCEMENT_PAGE}/{code}",
            retrieved_at=retrieved_at,
            raw_snapshot_path=raw_snapshot_path,
            raw_snapshot_sha256=raw_snapshot_sha256,
            parser_version=ANNOUNCEMENT_PARSER_VERSION,
        )
        for symbol in symbols
    ]


def _load_official_json(payload: bytes, context: str) -> dict[str, Any]:
    try:
        decoded = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Malformed official Binance JSON for {context}") from exc
    if not isinstance(decoded, dict) or decoded.get("code") != "000000":
        raise ValueError(f"Official Binance response failed for {context}")
    return decoded


def _read_with_rate_limit_retry(url: str, *, attempts: int = 10) -> bytes:
    for attempt in range(attempts):
        try:
            return _read_url(url)
        except urllib.error.HTTPError as exc:
            if exc.code != 429 or attempt == attempts - 1:
                raise
            retry_after = exc.headers.get("Retry-After") if exc.headers else None
            try:
                requested_wait = float(retry_after) if retry_after is not None else 0.0
            except ValueError:
                requested_wait = 0.0
            time.sleep(max(requested_wait, min(2 ** (attempt + 1), 60)))
    raise RuntimeError("Unreachable announcement retry state")


def _preserve_raw(path: Path, payload: bytes) -> None:
    if path.exists():
        if hashlib.sha256(path.read_bytes()).digest() != hashlib.sha256(payload).digest():
            raise FileExistsError(f"Existing raw announcement snapshot differs: {path}")
        return
    _write_bytes_exclusive_atomic(path, payload)


def acquire_announcement_corpus(
    raw_root: str | Path,
    archive_symbols: set[str],
    *,
    request_delay_seconds: float = 0.5,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Acquire public structured listing/delisting articles with immutable raw evidence."""
    root = Path(raw_root)
    root.mkdir(parents=True, exist_ok=True)
    rebuild_started_at = datetime.now(UTC).isoformat()
    evidence: list[AnnouncementEvidence] = []
    audit_catalogs: list[dict[str, Any]] = []
    detail_seen: set[str] = set()
    network_request_count = 0

    def read_source(url: str, pattern: str) -> bytes:
        nonlocal network_request_count
        existing = [
            path for path in root.glob(pattern) if not path.name.endswith(".provenance.json")
        ]
        if len(existing) > 1:
            raise ValueError(f"Multiple raw snapshots match resume pattern {pattern}")
        if existing:
            return existing[0].read_bytes()
        if network_request_count and network_request_count % 75 == 0:
            time.sleep(65)
        payload = _read_with_rate_limit_retry(url)
        network_request_count += 1
        return payload

    for event_type, catalog_id in (
        ("listing", LISTING_CATALOG_ID),
        ("delisting", DELISTING_CATALOG_ID),
    ):
        page = 1
        articles: list[dict[str, Any]] = []
        expected_total: int | None = None
        page_checksums: list[str] = []
        while expected_total is None or len(articles) < expected_total:
            url = (
                f"{ANNOUNCEMENT_API}/list/query?type=1&pageNo={page}"
                f"&pageSize={PAGE_SIZE}&catalogId={catalog_id}"
            )
            page_pattern = f"catalog_{catalog_id}_page_{page:03d}_*.json"
            page_was_cached = bool(list(root.glob(page_pattern)))
            payload = read_source(url, page_pattern)
            checksum = hashlib.sha256(payload).hexdigest()
            raw_path = root / f"catalog_{catalog_id}_page_{page:03d}_{checksum[:16]}.json"
            _preserve_raw(raw_path, payload)
            if not page_was_cached:
                record_new_snapshot_provenance(
                    raw_path,
                    url=url,
                    parser_version=ANNOUNCEMENT_PARSER_VERSION,
                )
            load_snapshot_provenance(raw_path, expected_url=url, expected_sha256=checksum)
            response = _load_official_json(payload, f"catalog {catalog_id} page {page}")
            catalogs = response.get("data", {}).get("catalogs")
            if not isinstance(catalogs, list) or len(catalogs) != 1:
                raise ValueError("Announcement catalog page has an unexpected structure")
            catalog = catalogs[0]
            if catalog.get("catalogId") != catalog_id or not isinstance(catalog.get("total"), int):
                raise ValueError("Announcement catalog identity or total is inconsistent")
            if expected_total is None:
                expected_total = catalog["total"]
            elif expected_total != catalog["total"]:
                raise ValueError("Announcement catalog total changed during pagination")
            page_articles = catalog.get("articles")
            if not isinstance(page_articles, list) or (not page_articles and len(articles) < expected_total):
                raise ValueError("Announcement pagination ended before its declared total")
            articles.extend(page_articles)
            page_checksums.append(checksum)
            page += 1
            if not page_was_cached:
                time.sleep(request_delay_seconds)
        if len(articles) != expected_total:
            raise ValueError("Announcement catalog returned more entries than its declared total")

        candidates = [
            article
            for article in articles
            if isinstance(article, dict)
            and isinstance(article.get("title"), str)
            and (event_type == "delisting" or _candidate_title(event_type, article["title"]))
        ]
        for candidate_position, article in enumerate(candidates, start=1):
            code = require_canonical_text(article.get("code"), "article code")
            if code in detail_seen:
                continue
            detail_seen.add(code)
            url = f"{ANNOUNCEMENT_API}/detail/query?articleCode={code}"
            article_pattern = f"article_{code}_*.json"
            article_was_cached = bool(list(root.glob(article_pattern)))
            payload = read_source(url, article_pattern)
            checksum = hashlib.sha256(payload).hexdigest()
            raw_path = root / f"article_{code}_{checksum[:16]}.json"
            _preserve_raw(raw_path, payload)
            if not article_was_cached:
                record_new_snapshot_provenance(
                    raw_path,
                    url=url,
                    parser_version=ANNOUNCEMENT_PARSER_VERSION,
                )
            article_provenance = load_snapshot_provenance(
                raw_path, expected_url=url, expected_sha256=checksum
            )
            response = _load_official_json(payload, f"article {code}")
            evidence.extend(
                parse_announcement_evidence(
                    response,
                    article,
                    event_type,
                    archive_symbols,
                    retrieved_at=(
                        article_provenance["original_retrieval_timestamp"]
                        if article_provenance is not None
                        else None
                    ),
                    raw_snapshot_path=str(raw_path.resolve()),
                    raw_snapshot_sha256=checksum,
                )
            )
            if not article_was_cached:
                time.sleep(request_delay_seconds)
            if candidate_position % 100 == 0:
                print(
                    f"Official {event_type} articles: "
                    f"{candidate_position}/{len(candidates)}",
                    flush=True,
                )
        audit_catalogs.append(
            {
                "catalog_id": catalog_id,
                "event_type": event_type,
                "declared_total": expected_total,
                "pages": page - 1,
                "candidate_articles": len(candidates),
                "inspection_policy": (
                    "complete_catalog_detail_inspection"
                    if event_type == "delisting"
                    else "strict_positive_listing_title_prefilter"
                ),
                "page_sha256s": page_checksums,
            }
        )

    rows = [asdict(item) for item in evidence]
    frame = pd.DataFrame.from_records(rows, columns=list(AnnouncementEvidence.__annotations__))
    # Multiple official articles for one symbol/event (for example a relisting) are
    # not silently collapsed into one contract identity.
    accepted = frame["match_status"].eq("accepted")
    duplicate = accepted & frame.loc[accepted].duplicated(
        ["event_type", "symbol"], keep=False
    ).reindex(frame.index, fill_value=False)
    frame.loc[duplicate, "match_status"] = "ambiguous_multiple_applicable_articles"
    return frame, {
        "rebuild_started_at": rebuild_started_at,
        "legacy_cached_retrieval_times_unresolved": int(frame["retrieved_at"].isna().sum()),
        "endpoint": ANNOUNCEMENT_API,
        "parser_version": ANNOUNCEMENT_PARSER_VERSION,
        "catalogs": audit_catalogs,
        "evidence_rows": len(frame),
        "accepted_rows": int(frame["match_status"].eq("accepted").sum()),
        "ambiguous_rows": int(duplicate.sum()),
        "network_requests_this_run": network_request_count,
    }
