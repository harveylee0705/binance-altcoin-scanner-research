from __future__ import annotations

import json

import pandas as pd

from alt_hot_scanner.data.announcements import parse_announcement_evidence
from alt_hot_scanner.universe.lifecycle import build_lifecycle_catalog, catalog_coverage


def _archive(symbol: str, first: str, last: str) -> dict:
    return {
        "symbol": symbol,
        "first_archive_month": first,
        "last_archive_month": last,
        "archive_discovery_timestamp": "2026-08-23T00:00:00+00:00",
        "archive_source_url": "https://official.example/index",
        "archive_discovery_provenance": "official_observed_bound_not_lifecycle_event",
        "archive_raw_snapshot_paths": [f"/{symbol}.xml"],
        "archive_raw_snapshot_sha256s": ["a" * 64],
    }


def _current(symbol: str) -> dict:
    base = symbol.removesuffix("USDT")
    return {
        "symbol": symbol,
        "base_asset": base,
        "quote_asset": "USDT",
        "margin_asset": "USDT",
        "market_family": "USDM",
        "product_family": "FUTURES",
        "contract_type": "PERPETUAL",
        "underlying_type": "COIN",
        "underlying_subtype": ("Layer-1",),
        "is_crypto_underlying": True,
        "is_stablecoin_underlying": False,
        "is_leveraged_token": False,
        "is_benchmark_btc": symbol == "BTCUSDT",
        "is_eth": symbol == "ETHUSDT",
        "scope_classification_status": "resolved_current_exchange_info",
        "scope_classification_provenance": "official_current_snapshot",
        "exchange_info_onboard_at": pd.Timestamp("2020-01-01T00:00:00Z"),
        "exchange_info_delivery_at": pd.Timestamp("2100-12-25T08:00:00Z"),
        "latest_known_status": "TRADING",
        "metadata_acquired_at": pd.Timestamp("2026-08-23T00:00:00Z"),
        "metadata_source": "https://fapi.binance.com/fapi/v1/exchangeInfo",
        "metadata_raw_snapshot_path": "/exchange.json",
        "metadata_raw_snapshot_sha256": "b" * 64,
    }


def _evidence(symbol: str, event_type: str, event_at: str | None) -> dict:
    return {
        "event_type": event_type,
        "symbol": symbol,
        "match_status": "accepted",
        "match_basis": "exact",
        "article_code": f"{event_type}-article",
        "article_title": event_type,
        "article_published_at": "2020-01-01T01:00:00+00:00",
        "official_event_at": event_at,
        "event_time_evidence_status": (
            "exact_official_announcement_time" if event_at else "unresolved"
        ),
        "source_url": "https://www.binance.com/en/support/announcement/detail/exact",
        "retrieved_at": "2026-08-23T00:00:00+00:00",
        "raw_snapshot_path": "/article.json",
        "raw_snapshot_sha256": "c" * 64,
        "parser_version": "test-v1",
    }


def test_catalog_keeps_official_current_and_archive_boundaries_distinct() -> None:
    archives = pd.DataFrame(
        [_archive("ETHUSDT", "2019-11", "2026-08"), _archive("OLDUSDT", "2021-01", "2022-02")]
    )
    current = pd.DataFrame([_current("ETHUSDT")])
    evidence = pd.DataFrame(
        [_evidence("ETHUSDT", "listing", "2019-11-29T08:00:00+00:00")]
    )
    catalog = build_lifecycle_catalog(archives, current, evidence)
    eth = catalog.loc[catalog["symbol"].eq("ETHUSDT")].iloc[0]
    old = catalog.loc[catalog["symbol"].eq("OLDUSDT")].iloc[0]
    assert eth["official_trading_start_at"] == "2019-11-29T08:00:00+00:00"
    assert eth["exchange_info_onboard_at"] == pd.Timestamp("2020-01-01T00:00:00Z")
    assert eth["first_archive_month"] == "2019-11"
    assert old["first_archive_month"] == "2021-01"
    assert old["official_trading_start_at"] is None
    assert old["scope_classification_status"] == "unresolved"
    coverage = catalog_coverage(catalog)
    assert coverage["total_archive_discovered_symbols"] == 2
    assert coverage["currently_quarantined"] == 2
    assert eth["onboard_start_discrepancy_status"] == "unresolved_material_discrepancy"


def test_structured_announcement_maps_multiple_exact_symbols_and_times() -> None:
    body = {
        "node": "root",
        "child": [
            {"node": "text", "text": "Binance Futures will launch:"},
            {"node": "text", "text": "2024-12-30 11:30 (UTC): PHAUSDT"},
            {"node": "text", "text": "2024-12-30 11:45 (UTC): DFUSDT"},
            {"node": "text", "text": "More details follow."},
        ],
    }
    detail = {
        "code": "000000",
        "data": {
            "title": "Binance Futures Will Launch PHAUSDT and DFUSDT Perpetual Contracts",
            "body": json.dumps(body),
        },
    }
    index = {"code": "official", "releaseDate": 1_735_556_100_000}
    parsed = parse_announcement_evidence(
        detail,
        index,
        "listing",
        {"PHAUSDT", "DFUSDT"},
        retrieved_at="2026-08-23T00:00:00+00:00",
        raw_snapshot_path="/raw.json",
        raw_snapshot_sha256="d" * 64,
    )
    assert [(item.symbol, item.official_event_at) for item in parsed] == [
        ("PHAUSDT", "2024-12-30T11:30:00+00:00"),
        ("DFUSDT", "2024-12-30T11:45:00+00:00"),
    ]


def test_announcement_never_infers_event_time_from_publication_or_title_date() -> None:
    detail = {
        "code": "000000",
        "data": {
            "title": "Binance Futures Launches ETH/USDT Perpetual Contract (2019-11-29)",
            "body": "<p>Binance Futures has launched ETH/USDT.</p>",
        },
    }
    parsed = parse_announcement_evidence(
        detail,
        {"code": "official", "releaseDate": 1_574_987_392_000},
        "listing",
        {"ETHUSDT"},
        retrieved_at="2026-08-23T00:00:00+00:00",
        raw_snapshot_path="/raw.json",
        raw_snapshot_sha256="e" * 64,
    )
    assert parsed[0].official_event_at is None
    assert parsed[0].event_time_evidence_status == "unresolved"


def test_title_identity_prevents_related_body_symbols_from_becoming_matches() -> None:
    detail = {
        "code": "000000",
        "data": {
            "title": "Binance Futures Launches BTCBUSD Perpetual Contract",
            "body": (
                "<p>BTCBUSD launches at 2021-01-12 14:00 (UTC).</p>"
                "<p>Related: switch from BTC/USDT to ETH/USDT.</p>"
            ),
        },
    }
    parsed = parse_announcement_evidence(
        detail,
        {"code": "official", "releaseDate": 1_610_377_124_000},
        "listing",
        {"BTCBUSD", "BTCUSDT", "ETHUSDT"},
        retrieved_at="2026-08-23T00:00:00+00:00",
        raw_snapshot_path="/raw.json",
        raw_snapshot_sha256="f" * 64,
    )
    assert [item.symbol for item in parsed] == ["BTCBUSD"]
