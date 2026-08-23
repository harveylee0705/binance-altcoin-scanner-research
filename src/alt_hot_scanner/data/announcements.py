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
from alt_hot_scanner.identity import require_binance_token, require_canonical_text

ANNOUNCEMENT_API = "https://www.binance.com/bapi/composite/v1/public/cms/article"
ANNOUNCEMENT_PAGE = "https://www.binance.com/en/support/announcement/detail"
ANNOUNCEMENT_PARSER_VERSION = "binance-announcement-v1"
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
    article_code: str
    article_title: str
    article_published_at: str
    official_event_at: str | None
    event_time_evidence_status: str
    source_url: str
    retrieved_at: str
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
    return re.sub(r"\s+", " ", html.unescape(" ".join(parts))).strip()


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
        parsed = pd.Timestamp(
            datetime.strptime(value, "%Y-%m-%d %I:%M %p").replace(tzinfo=UTC)
        )
    else:
        parsed = pd.Timestamp(value, tz="UTC")
    return parsed


def _lead_event_times(text: str) -> list[pd.Timestamp]:
    lowered = text.lower()
    boundaries = [
        position
        for marker in ("more details", "please note", "further information", "risk warning")
        if (position := lowered.find(marker)) >= 0
    ]
    lead = text[: min(boundaries)] if boundaries else text[:4000]
    matches = [*_DATE_FIRST.finditer(lead), *_TIME_FIRST.finditer(lead)]
    parsed: list[tuple[int, pd.Timestamp]] = []
    for match in matches:
        timestamp = _parse_timestamp(match)
        if timestamp not in [item[1] for item in parsed]:
            parsed.append((match.start(), timestamp))
    return [timestamp for _, timestamp in sorted(parsed)]


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
    retrieved_at: str,
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
    published = pd.to_datetime(index_record.get("releaseDate"), unit="ms", utc=True)
    times = _lead_event_times(body_text)
    mapped: dict[str, pd.Timestamp | None]
    if len(times) == 1:
        mapped = {symbol: times[0] for symbol in symbols}
    elif len(times) == len(symbols):
        mapped = dict(zip(symbols, times, strict=True))
    else:
        mapped = {symbol: None for symbol in symbols}

    code = require_canonical_text(index_record.get("code"), "article code")
    return [
        AnnouncementEvidence(
            event_type=event_type,
            symbol=symbol,
            match_status="accepted",
            match_basis="exact_canonical_symbol_or_exact_base_slash_usdt",
            article_code=code,
            article_title=title,
            article_published_at=published.isoformat(),
            official_event_at=mapped[symbol].isoformat() if mapped[symbol] is not None else None,
            event_time_evidence_status=(
                "exact_official_announcement_time" if mapped[symbol] is not None else "unresolved"
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
    retrieved = datetime.now(UTC).isoformat()
    evidence: list[AnnouncementEvidence] = []
    audit_catalogs: list[dict[str, Any]] = []
    detail_seen: set[str] = set()
    network_request_count = 0

    def read_source(url: str, pattern: str) -> bytes:
        nonlocal network_request_count
        existing = list(root.glob(pattern))
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
            and _candidate_title(event_type, article["title"])
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
            response = _load_official_json(payload, f"article {code}")
            evidence.extend(
                parse_announcement_evidence(
                    response,
                    article,
                    event_type,
                    archive_symbols,
                    retrieved_at=retrieved,
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
                "page_sha256s": page_checksums,
            }
        )

    rows = [asdict(item) for item in evidence]
    frame = pd.DataFrame.from_records(rows, columns=list(AnnouncementEvidence.__annotations__))
    # Multiple official articles for one symbol/event (for example a relisting) are
    # not silently collapsed into one contract identity.
    duplicate = frame.duplicated(["event_type", "symbol"], keep=False)
    frame.loc[duplicate, "match_status"] = "ambiguous_multiple_official_articles"
    return frame, {
        "retrieved_at": retrieved,
        "endpoint": ANNOUNCEMENT_API,
        "parser_version": ANNOUNCEMENT_PARSER_VERSION,
        "catalogs": audit_catalogs,
        "evidence_rows": len(frame),
        "accepted_rows": int(frame["match_status"].eq("accepted").sum()),
        "ambiguous_rows": int(duplicate.sum()),
        "network_requests_this_run": network_request_count,
    }
