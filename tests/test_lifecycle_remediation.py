from __future__ import annotations

import hashlib
import io
import json
import urllib.parse
import zipfile
from collections.abc import Callable
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path

import pandas as pd
import pytest

import alt_hot_scanner.data.binance_public as public_data
from alt_hot_scanner.data.announcements import (
    DELISTING_CATALOG_ID,
    LISTING_CATALOG_ID,
    acquire_announcement_corpus,
    parse_announcement_evidence,
)
from alt_hot_scanner.data.binance_public import (
    INDEX_HOST,
    _parse_s3_index_request,
    discover_archive_months,
    discover_frontier_daily_symbol_candidates,
)
from alt_hot_scanner.data.normalize import normalize_kline_frame
from alt_hot_scanner.data.provenance import (
    load_snapshot_provenance,
    record_new_snapshot_provenance,
)
from alt_hot_scanner.universe import evidence_replay
from alt_hot_scanner.universe.adjudications import episode_freshness_review_required
from alt_hot_scanner.universe.authorization import (
    APPROVAL_SCHEMA_VERSION,
    APPROVAL_STATE_SCHEMA_VERSION,
    PLAN_SCHEMA_VERSION,
    build_bundle_payload,
    build_lifecycle_freshness,
    build_plan_integrity,
    catalog_readiness,
    content_identity,
    sha256_path,
    validate_lifecycle_freshness,
    verify_bound_plan,
    verify_lifecycle_bundle,
)
from alt_hot_scanner.universe.contracts import records_from_exchange_info
from alt_hot_scanner.universe.delisting_registry import (
    cms_corpus_binding,
    verify_cms_corpus_binding,
    verify_delisting_records_against_announcement_evidence,
)
from alt_hot_scanner.universe.eligibility_oracle import (
    compare_production_catalog,
    derive_expected_eligibility,
    run_eligibility_oracle,
)
from alt_hot_scanner.universe.evidence_replay import _replay_announcements
from alt_hot_scanner.universe.lifecycle import (
    build_lifecycle_catalog,
    catalog_coverage,
)
from alt_hot_scanner.universe.scope_registry import (
    SCOPE_REVIEW_SCHEMA_VERSION,
    build_candidate_inventory,
    candidate_inventory_difference,
    scope_registry_identity,
)
from tests.scope_registry_fixtures import build_reviewed_scope_registry_fixture


def _announcement(
    title: str,
    lines: list[str],
    symbols: set[str],
    event_type: str = "listing",
):
    body = json.dumps({"child": [{"text": line} for line in lines]})
    return parse_announcement_evidence(
        {"code": "000000", "data": {"title": title, "body": body}},
        {"code": "article", "releaseDate": 1_735_556_100_000},
        event_type,
        symbols,
        retrieved_at=None,
        raw_snapshot_path="/raw.json",
        raw_snapshot_sha256="a" * 64,
    )


@pytest.mark.parametrize(
    ("title", "semantic_class"),
    [
        (
            "Binance Futures Copy Trading Adds ABCUSDT Perpetual Contract",
            "copy_trading_enablement",
        ),
        (
            "Binance Futures Trading Bots Add ABCUSDT Perpetual Contract",
            "trading_bot_enablement",
        ),
        (
            "Binance Futures Adds ABCUSDT to Multi-Assets Mode",
            "portfolio_margin_or_multi_asset_enablement",
        ),
        (
            "Binance Futures Updates Leverage and Margin Tiers for ABCUSDT Perpetual Contract",
            "contract_parameter_update",
        ),
    ],
)
def test_product_enablement_cannot_become_original_listing(
    title: str, semantic_class: str
) -> None:
    parsed = _announcement(
        title,
        ["ABCUSDT will be available at 2024-12-30 11:30 (UTC)."],
        {"ABCUSDT"},
    )
    assert parsed[0].article_semantic_class == semantic_class
    assert parsed[0].match_status == "rejected_semantic_class"
    assert parsed[0].official_event_at is None


def test_original_launch_requires_action_anchored_time() -> None:
    parsed = _announcement(
        "Binance Futures Will Launch ABCUSDT Perpetual Contract",
        [
            "The campaign begins at 2024-12-29 11:30 (UTC).",
            "Binance Futures will launch ABCUSDT perpetual contract.",
        ],
        {"ABCUSDT"},
    )
    assert parsed[0].match_status == "accepted"
    assert parsed[0].official_event_at is None


def test_launch_title_is_not_demoted_by_multi_asset_boilerplate() -> None:
    parsed = _announcement(
        "Binance Futures Will Launch ABCUSDT Perpetual Contract",
        [
            "Binance Futures will launch ABCUSDT at 2024-12-30 11:30 (UTC).",
            "The contract may support Multi-Assets Mode.",
        ],
        {"ABCUSDT"},
    )
    assert parsed[0].article_semantic_class == "original_perpetual_launch"
    assert parsed[0].official_event_at == "2024-12-30T11:30:00+00:00"


def test_multi_symbol_shared_and_distinct_launch_times_are_structurally_mapped() -> None:
    shared = _announcement(
        "Binance Futures Will Launch ABCUSDT and XYZUSDT Perpetual Contracts",
        [
            (
                "Binance Futures will launch ABCUSDT and XYZUSDT perpetual contracts at "
                "2024-12-30 11:30 (UTC)."
            )
        ],
        {"ABCUSDT", "XYZUSDT"},
    )
    assert {item.official_event_at for item in shared} == {"2024-12-30T11:30:00+00:00"}

    distinct = _announcement(
        "Binance Futures Will Launch ABCUSDT and XYZUSDT Perpetual Contracts",
        [
            "Binance Futures will launch:",
            "2024-12-30 11:30 (UTC): ABCUSDT",
            "2024-12-30 11:45 (UTC): XYZUSDT",
        ],
        {"ABCUSDT", "XYZUSDT"},
    )
    assert [(item.symbol, item.official_event_at) for item in distinct] == [
        ("ABCUSDT", "2024-12-30T11:30:00+00:00"),
        ("XYZUSDT", "2024-12-30T11:45:00+00:00"),
    ]


def test_ambiguous_multi_time_layout_stays_unresolved() -> None:
    parsed = _announcement(
        "Binance Futures Will Launch ABCUSDT and XYZUSDT Perpetual Contracts",
        [
            (
                "Binance Futures will launch ABCUSDT and XYZUSDT at "
                "2024-12-30 11:30 (UTC) after maintenance at "
                "2024-12-30 10:30 (UTC)."
            )
        ],
        {"ABCUSDT", "XYZUSDT"},
    )
    assert all(item.official_event_at is None for item in parsed)


def test_exact_delisting_publication_is_accepted_but_unrelated_delist_is_not() -> None:
    exact = _announcement(
        "Binance Futures Will Delist ABCUSDT Perpetual Contract",
        ["Binance Futures will delist ABCUSDT at 2024-12-30 11:30 (UTC)."],
        {"ABCUSDT"},
        "delisting",
    )
    assert exact[0].match_status == "accepted"
    assert exact[0].official_event_at == "2024-12-30T11:30:00+00:00"
    unrelated = _announcement(
        "Binance Spot Will Delist ABC",
        ["The ABCUSDT Futures contract is unaffected."],
        {"ABCUSDT"},
        "delisting",
    )
    assert unrelated[0].match_status == "rejected_semantic_class"


def _exchange_item(symbol: str, subtype: list[str] | None = None) -> dict:
    item = {
        "symbol": symbol,
        "baseAsset": symbol.removesuffix("USDT"),
        "quoteAsset": "USDT",
        "marginAsset": "USDT",
        "contractType": "PERPETUAL",
        "underlyingType": "COIN",
        "onboardDate": 1_600_000_000_000,
        "status": "TRADING",
    }
    if subtype is not None:
        item["underlyingSubType"] = subtype
    return item


def _review(asset: str, dimension: str, value: bool) -> dict:
    return {
        "asset": asset,
        "dimension": dimension,
        "value": value,
        "source_type": "reviewed_registry",
        "source_identifier": "official-review-1",
        "reviewed_parser_version": "registry-v1",
        "evidence_status": "accepted_reviewed",
    }


def test_absent_subtype_never_proves_negative_and_ustc_guard_is_positive() -> None:
    frame = records_from_exchange_info(
        {"symbols": [_exchange_item("USTCUSDT", ["Layer-1"])]},
        stablecoin_underlyings=["USTC"],
    )
    row = frame.iloc[0]
    assert bool(row["is_stablecoin_underlying"]) is True
    assert pd.isna(row["is_leveraged_token"])
    assert row["scope_classification_status"] == "unresolved"


def test_configured_stablecoin_conflicting_review_is_explicit() -> None:
    frame = records_from_exchange_info(
        {"symbols": [_exchange_item("USTCUSDT", ["Layer-1"])]},
        stablecoin_underlyings=["USTC"],
        reviewed_classification=[_review("USTC", "stablecoin_underlying", False)],
    )
    row = frame.iloc[0]
    assert pd.isna(row["is_stablecoin_underlying"])
    assert row["stablecoin_conflict_status"] == "conflict"


@pytest.mark.parametrize("value", [123.5, "123.5", True, float("nan"), float("inf")])
def test_fractional_nonfinite_and_boolean_kline_integers_are_rejected(value: object) -> None:
    row = [
        1_600_000_000_000,
        "1",
        "2",
        "0.5",
        "1.5",
        "10",
        1_600_003_599_999,
        "10",
        1,
        "5",
        "5",
        "0",
    ]
    row[0] = value
    with pytest.raises(ValueError, match="open_time"):
        normalize_kline_frame(pd.DataFrame([row]), "ABCUSDT")


def test_fractional_trade_count_is_rejected() -> None:
    row = [
        "1600002000000",
        "1",
        "2",
        "0.5",
        "1.5",
        "10",
        "1600005599999",
        "10",
        "123.5",
        "5",
        "5",
        "0",
    ]
    with pytest.raises(ValueError, match="trade_count"):
        normalize_kline_frame(pd.DataFrame([row]), "ABCUSDT")


@pytest.mark.parametrize("value", [123.5, "123.5", True, float("nan"), float("inf")])
@pytest.mark.parametrize("field", ["onboardDate", "deliveryDate"])
def test_exchange_info_lifecycle_milliseconds_are_strict(
    field: str, value: object
) -> None:
    item = _exchange_item("ABCUSDT", ["Layer-1"])
    item[field] = value
    with pytest.raises(ValueError, match=field):
        records_from_exchange_info({"symbols": [item]})


@pytest.mark.parametrize("value", [123.5, "123.5", True, float("nan"), float("inf")])
def test_cms_release_date_is_strict(value: object) -> None:
    body = json.dumps({"child": [{"text": "Binance Futures will launch ABCUSDT."}]})
    with pytest.raises(ValueError, match="releaseDate"):
        parse_announcement_evidence(
            {
                "code": "000000",
                "data": {
                    "title": "Binance Futures Will Launch ABCUSDT Perpetual Contract",
                    "body": body,
                },
            },
            {"code": "article", "releaseDate": value},
            "listing",
            {"ABCUSDT"},
            retrieved_at=None,
            raw_snapshot_path="/raw.json",
            raw_snapshot_sha256="a" * 64,
        )


def test_cached_snapshot_uses_original_sidecar_time_and_legacy_cache_stays_unresolved(
    tmp_path: Path,
) -> None:
    snapshot = tmp_path / "article.json"
    snapshot.write_bytes(b"official")
    digest = "6896191a14f6c66534bac457f50996b9330cd702cb6dbaae4c08d1d213e93d98"
    assert load_snapshot_provenance(
        snapshot, expected_url="https://official.example/article", expected_sha256=digest
    ) is None
    recorded = record_new_snapshot_provenance(
        snapshot,
        url="https://official.example/article",
        parser_version="test-v1",
        retrieved_at="2026-01-02T03:04:05+00:00",
    )
    loaded = load_snapshot_provenance(
        snapshot, expected_url="https://official.example/article", expected_sha256=digest
    )
    assert loaded == recorded
    assert loaded["original_retrieval_timestamp"] == "2026-01-02T03:04:05+00:00"


def test_archive_discovery_requires_actual_zip_not_checksum_sidecar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = "data/futures/um/monthly/klines/ABCUSDT/1h/ABCUSDT-1h-2020-01.zip"

    def page(keys: list[str]) -> bytes:
        contents = "".join(f"<Contents><Key>{item}</Key></Contents>" for item in keys)
        return (
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            f"<KeyCount>{len(keys)}</KeyCount><IsTruncated>false</IsTruncated>{contents}"
            "</ListBucketResult>"
        ).encode()

    monkeypatch.setattr(public_data, "_read_url", lambda url: page([f"{key}.CHECKSUM"]))
    with pytest.raises(ValueError, match="No monthly 1H archives"):
        discover_archive_months("ABCUSDT")
    monkeypatch.setattr(public_data, "_read_url", lambda url: page([key, f"{key}.CHECKSUM"]))
    observation = discover_archive_months("ABCUSDT")
    assert observation.observed_archive_object_keys == (key,)


def test_frontier_daily_candidate_discovery_is_complete_and_paginated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = "data/futures/um/daily/klines/"
    pages = [
        _candidate_index_page(
            [f"{base}DOSUSDT/", f"{base}ABCUSDT/"], truncated="true", token="next"
        ),
        _candidate_index_page([f"{base}ABCUSDT/", f"{base}TradFiUSDT/"]),
    ]
    requested: list[str] = []

    def read(url: str) -> bytes:
        requested.append(url)
        return pages[len(requested) - 1]

    monkeypatch.setattr(public_data, "_read_url", read)
    discovery = discover_frontier_daily_symbol_candidates()
    assert discovery.symbols == ("ABCUSDT", "DOSUSDT")
    assert discovery.quarantined_prefixes == ("TradFiUSDT",)
    assert discovery.audit.page_count == 2
    assert discovery.audit.unique_prefix_count == 3
    assert "continuation-token=next" in requested[1]


def _candidate_index_page(
    prefixes: list[str], *, truncated: str = "false", token: str | None = None
) -> bytes:
    contents = "".join(f"<CommonPrefixes><Prefix>{item}</Prefix></CommonPrefixes>" for item in prefixes)
    next_token = f"<NextContinuationToken>{token}</NextContinuationToken>" if token else ""
    return (
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        f"<KeyCount>{len(prefixes)}</KeyCount>{contents}"
        f"<IsTruncated>{truncated}</IsTruncated>{next_token}</ListBucketResult>"
    ).encode()


def _write_index_chain(
    tmp_path: Path,
    *,
    prefix: str,
    name: str,
    returned_tokens: list[str | None],
    delimiter: str | None = None,
    entries_on_terminal_page: list[str] | None = None,
    entries_are_prefixes: bool = False,
) -> tuple[list[Path], dict[str, str]]:
    """Write manifest-style S3 pages whose request tokens follow prior responses."""
    pages: list[Path] = []
    urls: dict[str, str] = {}
    requested_token: str | None = None
    entries = entries_on_terminal_page or []
    for page_number, next_token in enumerate([*returned_tokens, None], start=1):
        truncated = page_number <= len(returned_tokens)
        if entries and page_number == len(returned_tokens) + 1:
            if entries_are_prefixes:
                body = "".join(
                    f"<CommonPrefixes><Prefix>{entry}</Prefix></CommonPrefixes>"
                    for entry in entries
                )
            else:
                body = "".join(f"<Contents><Key>{entry}</Key></Contents>" for entry in entries)
        else:
            body = ""
        payload = (
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            f"<KeyCount>{len(entries) if body else 0}</KeyCount>{body}"
            f"<IsTruncated>{str(truncated).lower()}</IsTruncated>"
            f"{f'<NextContinuationToken>{next_token}</NextContinuationToken>' if next_token else ''}"
            "</ListBucketResult>"
        ).encode()
        digest = hashlib.sha256(payload).hexdigest()
        path = tmp_path / f"{name}_page_{page_number:03d}_{digest[:16]}.xml"
        path.write_bytes(payload)
        parameters = {"list-type": "2", "prefix": prefix}
        if delimiter is not None:
            parameters["delimiter"] = delimiter
        if requested_token is not None:
            parameters["continuation-token"] = requested_token
        url = f"{INDEX_HOST}?{urllib.parse.urlencode(parameters)}"
        record_new_snapshot_provenance(
            path,
            url=url,
            parser_version="fixture-index-v1",
            retrieved_at="2026-01-01T00:00:00+00:00",
        )
        pages.append(path)
        urls[str(path.resolve())] = url
        requested_token = next_token
    return pages, urls


def test_frontier_candidate_union_preserves_monthly_only_and_daily_only_identities() -> None:
    monthly = build_candidate_inventory(
        ["DELISTEDUSDT"],
        discovered_at="2026-01-01T00:00:00Z",
        source_identifier="monthly",
        discovery_layers=[{"layer": "historical_monthly_candidates"}],
    )
    union = build_candidate_inventory(
        ["DELISTEDUSDT", "DOSUSDT", "TradFiUSDT"],
        discovered_at="2026-01-01T00:00:00Z",
        source_identifier="monthly-and-daily",
        discovery_layers=[
            {"layer": "historical_monthly_candidates", "candidate_identities": ["DELISTEDUSDT"]},
            {"layer": "frontier_daily_candidates", "candidate_identities": ["DOSUSDT", "TradFiUSDT"]},
        ],
    )
    assert "DELISTEDUSDT" in union["candidate_identities"]
    assert {"DOSUSDT", "TradFiUSDT"}.issubset(union["candidate_identities"])
    assert "product_scope" not in union
    assert union["candidate_set_digest"] != monthly["candidate_set_digest"]
    assert candidate_inventory_difference(union, monthly)["added_candidates"] == [
        "DOSUSDT",
        "TradFiUSDT",
    ]


def test_daily_only_candidate_is_retained_as_an_explicit_blocking_catalog_row() -> None:
    monthly = _archive("AAAUSDT")
    daily_only = {
        **_archive("NEWUSDT"),
        "first_archive_month": None,
        "last_archive_month": None,
        "archive_discovery_provenance": (
            "frontier_daily_candidate_only_no_monthly_archive_blocking"
        ),
        "observed_archive_object_keys": [],
    }
    catalog = build_lifecycle_catalog(
        pd.DataFrame([monthly, daily_only]),
        pd.DataFrame([_current("AAAUSDT", "TRADING"), _current("NEWUSDT", "TRADING")]),
    ).set_index("symbol")
    assert set(catalog.index) == {"AAAUSDT", "NEWUSDT"}
    assert catalog.loc["NEWUSDT", "historical_inclusion_readiness"] == "blocked"
    assert catalog.loc["NEWUSDT", "current_status_warning"] == (
        "frontier_daily_only_no_monthly_archive"
    )


def test_daily_trade_index_rejects_missing_middle_page(tmp_path: Path) -> None:
    symbol = "ABCUSDT"
    prefix = f"data/futures/um/daily/trades/{symbol}/"

    def write_page(number: int, token: str | None, requested: str | None) -> Path:
        next_token = f"<NextContinuationToken>{token}</NextContinuationToken>" if token else ""
        key = f"{prefix}{symbol}-trades-2026-01-0{number}.zip"
        payload = (
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            f"<KeyCount>1</KeyCount><IsTruncated>{str(token is not None).lower()}</IsTruncated>"
            f"<Contents><Key>{key}</Key></Contents>{next_token}</ListBucketResult>"
        ).encode()
        path = tmp_path / f"{symbol}_page_{number:03d}_{hashlib.sha256(payload).hexdigest()[:16]}.xml"
        path.write_bytes(payload)
        query = f"?list-type=2&prefix={prefix}"
        if requested is not None:
            query += f"&continuation-token={requested}"
        record_new_snapshot_provenance(
            path,
            url=f"{INDEX_HOST}{query}",
            parser_version="test-v1",
        )
        return path

    pages = [write_page(1, "next", None), write_page(3, None, "next")]
    with pytest.raises(ValueError, match="missing or reordered page"):
        public_data.observed_daily_trade_keys_from_index_snapshots(
            [str(path) for path in pages], symbol
        )


def test_cms_corpus_binding_changes_when_article_detail_bytes_change() -> None:
    base = {
        "parser_version": "fixture-v1",
        "catalogs": [
            {
                "catalog_id": 161,
                "event_type": "delisting",
                "declared_total": 1,
                "pages": 1,
                "candidate_articles": 1,
                "inspection_policy": "complete_catalog_detail_inspection",
                "page_sha256s": ["a" * 64],
                "detail_sha256s": [{"article_code": "article", "sha256": "b" * 64}],
            }
        ],
    }
    changed = json.loads(json.dumps(base))
    changed["catalogs"][0]["detail_sha256s"][0]["sha256"] = "c" * 64
    assert cms_corpus_binding(base)["identity"] != cms_corpus_binding(changed)["identity"]


def _daily_row(symbol: str, dates: list[str]) -> dict:
    return {
        "symbol": symbol,
        "observed_daily_trade_dates": dates,
        "raw_snapshot_paths": [f"/raw/{symbol}.xml"],
        "raw_snapshot_sha256s": ["a" * 64],
    }


def _adjudication(symbol: str, episodes: list[dict], gap: dict | None = None) -> dict:
    value = {"symbol": symbol, "episodes": episodes}
    if gap is not None:
        value["gap_evidence"] = gap
    return {"by_symbol": {symbol: value}}


def test_episode_freshness_review_detects_terminal_resumption_and_interior_gap() -> None:
    terminal = _adjudication(
        "ABCUSDT",
        [{"episode_id": "ABCUSDT:1", "terminated_at": "2026-01-02T00:00:00+00:00"}],
    )
    result = episode_freshness_review_required(
        [_daily_row("ABCUSDT", ["2026-01-01", "2026-01-02", "2026-01-03"])],
        terminal,
        required_valid_through_utc="2026-01-04T00:00:00Z",
    )
    assert result is not None
    assert "after a reviewed terminal episode" in result["findings"][0]["reason"]

    open_episode = _adjudication(
        "ABCUSDT", [{"episode_id": "ABCUSDT:1", "terminated_at": None}]
    )
    result = episode_freshness_review_required(
        [_daily_row("ABCUSDT", ["2026-01-01", "2026-01-03"])],
        open_episode,
        required_valid_through_utc="2026-01-04T00:00:00Z",
    )
    assert result is not None
    assert "interior daily-trade discontinuity" in result["findings"][0]["reason"]


def test_reviewed_relist_gap_passes_and_open_frontier_deficiency_stops() -> None:
    gap = {
        "last_pre_gap_trade_archive_date": "2026-01-01",
        "first_post_gap_trade_archive_date": "2026-01-04",
    }
    reviewed = _adjudication(
        "ABCUSDT",
        [
            {"episode_id": "ABCUSDT:1", "terminated_at": "2026-01-02T00:00:00+00:00"},
            {"episode_id": "ABCUSDT:2", "terminated_at": "2026-01-05T00:00:00+00:00"},
        ],
        gap,
    )
    assert episode_freshness_review_required(
        [_daily_row("ABCUSDT", ["2026-01-01", "2026-01-04", "2026-01-05"])],
        reviewed,
        required_valid_through_utc="2026-01-06T00:00:00Z",
        reviewed_delisting_records=[_accepted_terminal_cutoff()],
    ) is None

    open_episode = _adjudication(
        "ABCUSDT", [{"episode_id": "ABCUSDT:1", "terminated_at": None}]
    )
    result = episode_freshness_review_required(
        [_daily_row("ABCUSDT", ["2026-01-01", "2026-01-02"])],
        open_episode,
        required_valid_through_utc="2026-01-04T00:00:00Z",
    )
    assert result is not None
    assert "frontier daily-trade evidence" in result["findings"][0]["reason"]


def _accepted_terminal_cutoff(
    symbol: str = "ABCUSDT", episode_id: str = "ABCUSDT:1"
) -> dict:
    return {
        "symbol": symbol,
        "lifecycle_episode_id": episode_id,
        "review_status": "accepted_exact_cutoff",
        "terminal_last_trading_at": "2026-01-02T15:00:00Z",
    }


def test_registry_terminal_detects_short_same_symbol_relist() -> None:
    adjudication = _adjudication(
        "ABCUSDT", [{"episode_id": "ABCUSDT:1", "terminated_at": None}]
    )
    result = episode_freshness_review_required(
        [_daily_row("ABCUSDT", ["2026-01-02", "2026-01-03"])],
        adjudication,
        required_valid_through_utc="2026-01-04T00:00:00Z",
        reviewed_delisting_records=[_accepted_terminal_cutoff()],
    )
    assert result is not None
    assert "reviewed registry terminal" in result["findings"][0]["reason"]


def test_registry_terminal_detects_consecutive_date_relist() -> None:
    adjudication = _adjudication(
        "ABCUSDT", [{"episode_id": "ABCUSDT:1", "terminated_at": None}]
    )
    result = episode_freshness_review_required(
        [_daily_row("ABCUSDT", ["2026-01-01", "2026-01-02", "2026-01-03"])],
        adjudication,
        required_valid_through_utc="2026-01-04T00:00:00Z",
        reviewed_delisting_records=[_accepted_terminal_cutoff()],
    )
    assert result is not None
    assert result["status"] == "review_required"


def test_registry_terminal_closes_implicit_episode_without_frontier_requirement() -> None:
    assert episode_freshness_review_required(
        [_daily_row("ABCUSDT", ["2026-01-02"])],
        {"by_symbol": {}},
        required_valid_through_utc="2026-01-04T00:00:00Z",
        reviewed_delisting_records=[_accepted_terminal_cutoff()],
    ) is None


def test_reviewed_later_episode_suppresses_registry_terminal_relist_finding() -> None:
    adjudication = _adjudication(
        "ABCUSDT",
        [
            {"episode_id": "ABCUSDT:1", "terminated_at": None},
            {"episode_id": "ABCUSDT:2", "terminated_at": None},
        ],
    )
    assert episode_freshness_review_required(
        [_daily_row("ABCUSDT", ["2026-01-01", "2026-01-02", "2026-01-03"])],
        adjudication,
        required_valid_through_utc="2026-01-04T00:00:00Z",
        reviewed_delisting_records=[_accepted_terminal_cutoff()],
    ) is None


@pytest.mark.parametrize(
    "review_status",
    ["accepted_exact_cutoff", "reviewed_no_reliable_cutoff"],
)
def test_publication_or_null_terminal_does_not_invent_relist_boundary(
    review_status: str,
) -> None:
    cutoff = _accepted_terminal_cutoff()
    cutoff["review_status"] = review_status
    cutoff["official_publication_timestamp"] = "2026-01-01T00:00:00Z"
    cutoff["terminal_last_trading_at"] = None
    adjudication = _adjudication(
        "ABCUSDT", [{"episode_id": "ABCUSDT:1", "terminated_at": None}]
    )
    assert episode_freshness_review_required(
        [_daily_row("ABCUSDT", ["2026-01-01", "2026-01-02", "2026-01-03"])],
        adjudication,
        required_valid_through_utc="2026-01-04T00:00:00Z",
        reviewed_delisting_records=[cutoff],
    ) is None


def test_freshness_common_horizon_and_required_boundary_are_fail_closed(tmp_path: Path) -> None:
    raw = tmp_path / "server.json"
    raw.write_text('{"serverTime": 1767312000000}')
    descriptor = {
        "path": str(raw.resolve()),
        "sha256": sha256_path(raw),
        "source_url": "https://fapi.binance.com/fapi/v1/time",
        "server_time_utc": "2026-01-02T00:00:00Z",
        "server_date_utc": "2026-01-02",
        "maximum_archive_backed_horizon_utc": "2026-01-01T00:00:00Z",
    }
    freshness = build_lifecycle_freshness(
        required_valid_through_utc="2026-01-01T00:00:00Z",
        candidate_valid_through_utc="2026-01-01T00:00:00Z",
        episode_valid_through_utc="2026-01-01T00:00:00Z",
        delisting_valid_through_utc="2026-01-01T01:00:00Z",
        server_time_evidence=descriptor,
        frontier_candidate_discovery={"layers": [], "candidate_set_digest": "a" * 64},
        episode_freshness_evidence={"symbols": []},
        announcement_corpus={
            "identity": "b" * 64,
            "sha256": "b" * 64,
            "acquisition_started_at_utc": "2026-01-02T00:00:00Z",
        },
        bound_raw_evidence=[{"path": str(raw.resolve()), "sha256": descriptor["sha256"]}],
    )
    assert freshness["lifecycle_evidence_valid_through_utc"] == "2026-01-01T00:00:00Z"
    validate_lifecycle_freshness(freshness, report_root=tmp_path)
    with pytest.raises(ValueError, match="required freshness horizon"):
        build_lifecycle_freshness(
            candidate_valid_through_utc=freshness["candidate_valid_through_utc"], episode_valid_through_utc=freshness["episode_valid_through_utc"], delisting_valid_through_utc=freshness["delisting_valid_through_utc"], server_time_evidence=freshness["binance_server_time_evidence"], frontier_candidate_discovery=freshness["frontier_candidate_discovery"], episode_freshness_evidence=freshness["episode_freshness_evidence"], announcement_corpus=freshness["announcement_corpus"], bound_raw_evidence=freshness["bound_raw_evidence"], required_valid_through_utc="2026-01-02T00:00:00Z"
        )


def _archive(symbol: str) -> dict:
    return {
        "symbol": symbol,
        "first_archive_month": "2020-01",
        "last_archive_month": "2020-01",
        "archive_discovery_timestamp": "2026-01-01T00:00:00+00:00",
        "archive_source_url": "https://official.example/index",
        "archive_discovery_provenance": "official_observation",
        "observed_archive_object_keys": [
            f"data/futures/um/monthly/klines/{symbol}/1h/{symbol}-1h-2020-01.zip"
        ],
    }


def _current(symbol: str, status: str) -> dict:
    return {
        "symbol": symbol,
        "base_asset": symbol.removesuffix("USDT"),
        "quote_asset": "USDT",
        "margin_asset": "USDT",
        "market_family": "USDM",
        "product_family": "FUTURES",
        "contract_type": "PERPETUAL",
        "underlying_type": "COIN",
        "underlying_subtype": ("reviewed",),
        "is_crypto_underlying": True,
        "is_stablecoin_underlying": False,
        "is_leveraged_token": False,
        "stablecoin_evidence_status": "accepted_reviewed_evidence",
        "stablecoin_conflict_status": "none",
        "leveraged_evidence_status": "accepted_reviewed_evidence",
        "leveraged_conflict_status": "none",
        "is_benchmark_btc": False,
        "is_eth": False,
        "scope_classification_status": "resolved_reviewed_evidence",
        "scope_classification_provenance": "reviewed_registry",
        "exchange_info_onboard_at": pd.Timestamp("2020-01-01T00:00:00Z"),
        "latest_known_status": status,
        "metadata_acquired_at": None,
        "metadata_source": "official",
        "metadata_raw_snapshot_path": "/exchange.json",
        "metadata_raw_snapshot_sha256": "b" * 64,
    }


def _listing(symbol: str) -> dict:
    return {
        "event_type": "listing",
        "symbol": symbol,
        "match_status": "accepted",
        "article_published_at": "2019-12-31T00:00:00+00:00",
        "official_event_at": "2020-01-01T00:00:00+00:00",
        "event_time_evidence_status": "action_symbol_time_anchored",
        "source_url": "https://official.example/listing",
        "article_code": "listing",
        "retrieved_at": None,
        "raw_snapshot_path": "/listing.json",
        "raw_snapshot_sha256": "c" * 64,
        "parser_version": "test-v1",
    }


def _delisting_evidence(
    *,
    symbol: str = "AAAUSDT",
    article_code: str = "delist-a",
    article_published_at: str = "2024-12-30T11:30:00+00:00",
    official_event_at: str | None = "2024-12-31T11:30:00+00:00",
    source_url: str | None = None,
    raw_snapshot_sha256: str = "a" * 64,
    match_status: str = "accepted",
    article_semantic_class: str = "delisting_or_settlement",
    semantic_evidence_status: str = "accepted_positive_semantic_evidence",
    event_time_evidence_status: str | None = "action_symbol_time_anchored",
) -> dict:
    return {
        "event_type": "delisting",
        "symbol": symbol,
        "match_status": match_status,
        "article_semantic_class": article_semantic_class,
        "semantic_evidence_status": semantic_evidence_status,
        "article_code": article_code,
        "article_published_at": article_published_at,
        "official_event_at": official_event_at,
        "event_time_evidence_status": event_time_evidence_status,
        "source_url": source_url or f"https://www.binance.com/en/support/announcement/{article_code}",
        "raw_snapshot_sha256": raw_snapshot_sha256,
    }


def _accepted_delisting_registry(evidence: dict) -> dict:
    return {
        "symbol": evidence["symbol"],
        "lifecycle_episode_id": f'{evidence["symbol"]}:1',
        "article_code": evidence["article_code"],
        "official_article_url": evidence["source_url"],
        "raw_article_sha256": evidence["raw_snapshot_sha256"],
        "official_publication_timestamp": evidence["article_published_at"],
        "terminal_last_trading_at": evidence["official_event_at"],
        "product_event_disposition": "binance_usdm_futures_termination",
        "review_status": "accepted_exact_cutoff",
    }


def test_accepted_delisting_record_binds_exact_replayed_evidence() -> None:
    evidence = _delisting_evidence()
    registry = {"records": [_accepted_delisting_registry(evidence)]}

    verify_delisting_records_against_announcement_evidence(registry, [evidence])


def test_accepted_delisting_record_allows_rejected_same_symbol_articles() -> None:
    evidence = _delisting_evidence()
    unrelated = _delisting_evidence(
        article_code="delist-b",
        raw_snapshot_sha256="b" * 64,
        match_status="rejected_semantic_class",
        article_semantic_class="rejected_semantic_class",
        semantic_evidence_status="rejected_not_applicable_event_semantics",
        official_event_at=None,
        event_time_evidence_status="unresolved",
    )
    registry = {"records": [_accepted_delisting_registry(evidence)]}

    verify_delisting_records_against_announcement_evidence(registry, [evidence, unrelated])


def test_accepted_delisting_record_requires_exact_row_with_multiple_same_symbol_articles() -> None:
    evidence = _delisting_evidence()
    unrelated = [
        _delisting_evidence(article_code=f"delist-{code}", raw_snapshot_sha256=code * 64)
        for code in ("b", "c", "d")
    ]
    registry = {"records": [_accepted_delisting_registry(evidence)]}

    verify_delisting_records_against_announcement_evidence(registry, [evidence, *unrelated])


@pytest.mark.parametrize(
    "unrelated",
    [
        _delisting_evidence(symbol="BBBUSDT"),
        _delisting_evidence(article_code="delist-b"),
        {
            **_delisting_evidence(),
            "event_type": "listing",
        },
    ],
)
def test_non_exact_candidate_tuple_does_not_satisfy_accepted_record(
    unrelated: dict,
) -> None:
    expected = _delisting_evidence()
    registry = {"records": [_accepted_delisting_registry(expected)]}

    with pytest.raises(ValueError):
        verify_delisting_records_against_announcement_evidence(registry, [unrelated])


def test_zero_exact_candidate_tuples_fail_closed() -> None:
    expected = _delisting_evidence()
    registry = {"records": [_accepted_delisting_registry(expected)]}

    with pytest.raises(ValueError):
        verify_delisting_records_against_announcement_evidence(registry, [])


@pytest.mark.parametrize(
    "mutation",
    [
        "article_code",
        "raw_article_sha256",
        "symbol",
        "official_publication_timestamp",
        "official_article_url",
    ],
)
def test_accepted_delisting_record_rejects_per_record_binding_tampering(
    mutation: str,
) -> None:
    evidence = _delisting_evidence()
    registry = {"records": [_accepted_delisting_registry(evidence)]}
    record = registry["records"][0]
    if mutation == "article_code":
        record["article_code"] = "delist-b"
    elif mutation == "raw_article_sha256":
        record["raw_article_sha256"] = "b" * 64
    elif mutation == "symbol":
        record["symbol"] = "BBBUSDT"
    elif mutation == "official_publication_timestamp":
        record["official_publication_timestamp"] = "2024-12-30T11:30:01Z"
    else:
        record["official_article_url"] = "https://www.binance.com/en/support/announcement/other"

    with pytest.raises(ValueError):
        verify_delisting_records_against_announcement_evidence(registry, [evidence])


def test_real_other_article_from_same_corpus_cannot_replace_bound_article() -> None:
    evidence = _delisting_evidence()
    other_article = _delisting_evidence(
        symbol="BBBUSDT", article_code="delist-b", raw_snapshot_sha256="b" * 64
    )
    registry = {"records": [_accepted_delisting_registry(evidence)]}
    registry["records"][0]["article_code"] = other_article["article_code"]

    with pytest.raises(ValueError):
        verify_delisting_records_against_announcement_evidence(
            registry, [evidence, other_article]
        )


def test_real_other_detail_sha_cannot_substitute_for_bound_article() -> None:
    evidence = _delisting_evidence()
    registry = {"records": [_accepted_delisting_registry(evidence)]}
    registry["records"][0]["raw_article_sha256"] = "b" * 64
    corpus_audit = {
        "parser_version": "fixture-v1",
        "catalogs": [
            {
                "event_type": "delisting",
                "catalog_id": 161,
                "declared_total": 2,
                "pages": 1,
                "candidate_articles": 2,
                "inspection_policy": "complete_catalog_detail_inspection",
                "page_sha256s": ["c" * 64],
                "detail_sha256s": [
                    {"article_code": "delist-a", "sha256": "a" * 64},
                    {"article_code": "delist-b", "sha256": "b" * 64},
                ],
            }
        ],
    }
    before = cms_corpus_binding(corpus_audit)

    with pytest.raises(ValueError):
        verify_delisting_records_against_announcement_evidence(registry, [evidence])

    assert cms_corpus_binding(corpus_audit) == before


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("article_semantic_class", "rejected_semantic_class"),
        ("semantic_evidence_status", "rejected_not_applicable_event_semantics"),
        ("match_status", "ambiguous_multiple_applicable_articles"),
    ],
)
def test_nonaccepted_delisting_evidence_cannot_support_exact_cutoff(
    field: str, value: str
) -> None:
    evidence = _delisting_evidence(**{field: value})
    registry = {"records": [_accepted_delisting_registry(evidence)]}

    with pytest.raises(ValueError):
        verify_delisting_records_against_announcement_evidence(registry, [evidence])


def test_accepted_delisting_record_rejects_terminal_timestamp_mismatch() -> None:
    evidence = _delisting_evidence()
    registry = {"records": [_accepted_delisting_registry(evidence)]}
    registry["records"][0]["terminal_last_trading_at"] = "2024-12-31T11:30:01Z"

    with pytest.raises(ValueError):
        verify_delisting_records_against_announcement_evidence(registry, [evidence])


def test_accepted_delisting_record_accepts_valid_terminal_timestamp_match() -> None:
    evidence = _delisting_evidence()
    registry = {"records": [_accepted_delisting_registry(evidence)]}
    registry["records"][0]["terminal_last_trading_at"] = "2024-12-31T18:30:00+07:00"

    verify_delisting_records_against_announcement_evidence(registry, [evidence])


def test_null_terminal_timestamp_is_not_invented() -> None:
    evidence = _delisting_evidence(official_event_at=None, event_time_evidence_status="unresolved")
    registry = {"records": [_accepted_delisting_registry(evidence)]}
    assert registry["records"][0]["terminal_last_trading_at"] is None

    verify_delisting_records_against_announcement_evidence(registry, [evidence])

    assert registry["records"][0]["terminal_last_trading_at"] is None


def test_null_registry_terminal_rejects_nonnull_evidence_terminal() -> None:
    evidence = _delisting_evidence()
    registry = {"records": [_accepted_delisting_registry(evidence)]}
    registry["records"][0]["terminal_last_trading_at"] = None

    with pytest.raises(ValueError):
        verify_delisting_records_against_announcement_evidence(registry, [evidence])


def test_nonnull_registry_terminal_rejects_null_evidence_terminal() -> None:
    evidence = _delisting_evidence(
        official_event_at=None, event_time_evidence_status="unresolved"
    )
    registry = {"records": [_accepted_delisting_registry(evidence)]}
    registry["records"][0]["terminal_last_trading_at"] = "2024-12-31T11:30:00Z"

    with pytest.raises(ValueError):
        verify_delisting_records_against_announcement_evidence(registry, [evidence])


def test_nonnull_registry_terminal_requires_positive_anchored_event_status() -> None:
    evidence = _delisting_evidence(event_time_evidence_status="unresolved")
    registry = {"records": [_accepted_delisting_registry(evidence)]}

    with pytest.raises(ValueError):
        verify_delisting_records_against_announcement_evidence(registry, [evidence])


def test_multiple_exact_candidate_rows_fail_closed() -> None:
    evidence = _delisting_evidence()
    registry = {"records": [_accepted_delisting_registry(evidence)]}

    with pytest.raises(ValueError):
        verify_delisting_records_against_announcement_evidence(
            registry, [evidence, deepcopy(evidence)]
        )


def _run_cached_live_and_replay(
    root: Path, *, second_title: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    def write_catalog(catalog_id: int, articles: list[dict]) -> Path:
        path = root / f"catalog_{catalog_id}_page_001_fixture.json"
        path.write_text(
            json.dumps(
                {
                    "code": "000000",
                    "data": {
                        "catalogs": [
                            {
                                "catalogId": catalog_id,
                                "total": len(articles),
                                "articles": articles,
                            }
                        ]
                    },
                }
            )
        )
        return path

    def write_detail(code: str, title: str, body_text: str) -> Path:
        path = root / f"article_{code}_fixture.json"
        path.write_text(
            json.dumps(
                {
                    "code": "000000",
                    "data": {
                        "title": title,
                        "body": json.dumps(
                            {
                                "child": [
                                    {"text": body_text}
                                ]
                            }
                        ),
                    },
                }
            )
        )
        return path

    root.mkdir()
    articles = [
        {"code": code, "title": "fixture", "releaseDate": 1_735_556_100_000}
        for code in ("delist-a", "delist-b")
    ]
    listing_catalog = write_catalog(LISTING_CATALOG_ID, [])
    delisting_catalog = write_catalog(DELISTING_CATALOG_ID, articles)
    accepted_title = "Binance Futures Will Delist AAAUSDT Perpetual Contract"
    accepted_body = "Binance Futures will delist AAAUSDT at 2024-12-31 11:30 (UTC)."
    detail_paths = [
        write_detail("delist-a", accepted_title, accepted_body),
        write_detail(
            "delist-b",
            second_title,
            (
                accepted_body
                if second_title == accepted_title
                else "The AAAUSDT Futures contract is unaffected."
            ),
        ),
    ]
    entries = [
        {
            "evidence_role": "cms_catalog_response",
            "path": str(listing_catalog),
            "original_retrieval_timestamp": None,
        },
        {
            "evidence_role": "cms_catalog_response",
            "path": str(delisting_catalog),
            "original_retrieval_timestamp": None,
        },
        *[
            {
                "evidence_role": "cms_article_detail_response",
                "path": str(path),
                "original_retrieval_timestamp": "2026-01-01T00:00:00+00:00",
            }
            for path in detail_paths
        ],
    ]
    live, _ = acquire_announcement_corpus(root, {"AAAUSDT"}, request_delay_seconds=0)
    replayed, _ = _replay_announcements(entries, {"AAAUSDT"})
    return live, replayed


@pytest.mark.parametrize(
    ("second_title", "expected_statuses"),
    [
        (
            "Binance Futures Will Delist AAAUSDT Perpetual Contract",
            ["ambiguous_multiple_applicable_articles"] * 2,
        ),
        (
            "Binance Spot Will Delist ABC",
            ["accepted", "rejected_semantic_class"],
        ),
    ],
)
def test_replay_match_status_matches_live_acquisition(
    tmp_path: Path, second_title: str, expected_statuses: list[str]
) -> None:
    live, replayed = _run_cached_live_and_replay(
        tmp_path / "cms", second_title=second_title
    )

    live_statuses = live["match_status"].tolist()
    replay_statuses = replayed["match_status"].tolist()
    assert live_statuses == expected_statuses
    assert replay_statuses == live_statuses


def test_full_evidence_replay_rejects_competing_accepted_same_symbol_article(
    tmp_path: Path,
) -> None:
    _, replayed = _run_cached_live_and_replay(
        tmp_path / "cms",
        second_title="Binance Futures Will Delist AAAUSDT Perpetual Contract",
    )
    rows = replayed.to_dict("records")
    registry = {"records": [_accepted_delisting_registry(rows[0])]}

    with pytest.raises(ValueError):
        verify_delisting_records_against_announcement_evidence(registry, rows)


def test_full_evidence_replay_allows_rejected_same_symbol_article(
    tmp_path: Path,
) -> None:
    _, replayed = _run_cached_live_and_replay(
        tmp_path / "cms", second_title="Binance Spot Will Delist ABC"
    )
    rows = replayed.to_dict("records")
    registry = {"records": [_accepted_delisting_registry(rows[0])]}

    verify_delisting_records_against_announcement_evidence(registry, rows)


def test_corpus_identity_alone_does_not_authorize_tampered_accepted_record() -> None:
    evidence = _delisting_evidence()
    corpus_audit = {
        "parser_version": "fixture-v1",
        "catalogs": [
            {
                "event_type": "delisting",
                "catalog_id": 161,
                "declared_total": 1,
                "pages": 1,
                "candidate_articles": 1,
                "inspection_policy": "complete_catalog_detail_inspection",
                "page_sha256s": ["c" * 64],
                "detail_sha256s": [{"article_code": "delist-a", "sha256": "a" * 64}],
            }
        ],
    }
    registry_record = _accepted_delisting_registry(evidence)
    registry_record["raw_article_sha256"] = "b" * 64
    registry = {
        "official_cms_corpus": cms_corpus_binding(corpus_audit),
        "records": [registry_record],
    }

    verify_cms_corpus_binding(registry, corpus_audit)
    with pytest.raises(ValueError):
        verify_delisting_records_against_announcement_evidence(registry, [evidence])


def test_negative_review_dispositions_remain_unchanged() -> None:
    registry = {
        "records": [
            {
                "symbol": "AAAUSDT",
                "lifecycle_episode_id": "AAAUSDT:1",
                "review_status": status,
                "official_publication_timestamp": None,
                "terminal_last_trading_at": None,
            }
            for status in ("reviewed_no_reliable_cutoff", "not_applicable_current_episode")
        ]
    }
    before = deepcopy(registry)

    verify_delisting_records_against_announcement_evidence(registry, [])

    assert registry == before


def test_completed_delisting_search_without_timestamp_does_not_remove_history() -> None:
    archives = pd.DataFrame([_archive("AAAUSDT"), _archive("BBBUSDT")])
    current = pd.DataFrame([_current("AAAUSDT", "TRADING"), _current("BBBUSDT", "SETTLING")])
    evidence = pd.DataFrame([_listing("AAAUSDT"), _listing("BBBUSDT")])
    trades = pd.DataFrame(
        [
            {
                "symbol": symbol,
                "archive_object_key": f"data/futures/um/daily/trades/{symbol}/{symbol}-trades-2020-01-01.zip",
                "published_sha256": "d" * 64,
                "computed_sha256": "d" * 64,
                "raw_path": f"/{symbol}.zip",
                "original_retrieval_timestamp": None,
                "earliest_trade_timestamp": "2020-01-01T00:00:01+00:00",
                "parser_version": "test",
                "evidence_status": "checksum_verified_official_binance_futures_trade",
            }
            for symbol in ("AAAUSDT", "BBBUSDT")
        ]
    )
    registry = [
        {
            "symbol": symbol,
            "lifecycle_episode_id": f"{symbol}:1",
            "official_publication_timestamp": None,
            "terminal_last_trading_at": None,
            "official_article_url": None,
            "article_code": None,
            "raw_article_sha256": None,
            "review_status": status,
        }
        for symbol, status in (
            ("AAAUSDT", "not_applicable_current_episode"),
            ("BBBUSDT", "reviewed_no_reliable_cutoff"),
        )
    ]
    catalog = build_lifecycle_catalog(
        archives,
        current,
        evidence,
        announcement_search_completed=True,
        first_observed_trades=trades,
        delisting_registry_records=registry,
    ).set_index("symbol")
    assert catalog.loc["AAAUSDT", "historical_inclusion_readiness"] == "ready"
    assert catalog.loc["AAAUSDT", "delisting_evidence_state"] == "not_applicable_currently_trading"
    assert catalog.loc["BBBUSDT", "historical_inclusion_readiness"] == "ready"
    assert catalog.loc["BBBUSDT", "delisting_evidence_state"] == (
        "official_search_completed_no_reliable_announcement_timestamp"
    )


def test_catalog_rejects_reviewed_terminal_with_later_current_state_without_relist() -> None:
    trade = {
        "symbol": "AAAUSDT",
        "archive_object_key": "data/futures/um/daily/trades/AAAUSDT/AAAUSDT-trades-2020-01-01.zip",
        "published_sha256": "d" * 64,
        "computed_sha256": "d" * 64,
        "raw_path": "/AAAUSDT.zip",
        "original_retrieval_timestamp": None,
        "earliest_trade_timestamp": "2020-01-01T00:00:01+00:00",
        "parser_version": "test",
        "evidence_status": "checksum_verified_official_binance_futures_trade",
    }
    adjudication = {
        "adjudication_id": "reviewed",
        "by_symbol": {
            "AAAUSDT": {
                "symbol": "AAAUSDT",
                "episodes": [{"episode_id": "AAAUSDT:1", "terminated_at": None}],
            }
        },
    }
    cutoff = _accepted_terminal_cutoff("AAAUSDT", "AAAUSDT:1")
    with pytest.raises(ValueError, match="later-current state"):
        build_lifecycle_catalog(
            pd.DataFrame([_archive("AAAUSDT")]),
            pd.DataFrame([_current("AAAUSDT", "TRADING")]),
            pd.DataFrame([_listing("AAAUSDT")]),
            created_at=pd.Timestamp("2026-01-04T00:00:00Z"),
            announcement_search_completed=True,
            first_observed_trades=pd.DataFrame([trade]),
            episode_first_observed_trades=[{**trade, "lifecycle_episode_id": "AAAUSDT:1"}],
            delisting_registry_records=[cutoff],
            lifecycle_adjudications=adjudication,
        )


def _write_bundle(tmp_path: Path) -> tuple[Path, pd.DataFrame]:
    (tmp_path / "config").mkdir()
    review_dir = tmp_path / "docs" / "reviews"
    review_dir.mkdir(parents=True)
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    trade_path = raw_dir / "AAAUSDT.zip"
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w") as archive:
        archive.writestr(
            "AAAUSDT-trades-2020-01-01.csv",
            "id,price,qty,quote_qty,time\n1,1,1,1,1577836801000\n",
        )
    trade_path.write_bytes(payload.getvalue())
    trade_hash = hashlib.sha256(payload.getvalue()).hexdigest()
    trade = {
        "symbol": "AAAUSDT",
        "archive_object_key": (
            "data/futures/um/daily/trades/AAAUSDT/AAAUSDT-trades-2020-01-01.zip"
        ),
        "archive_date": "2020-01-01",
        "published_sha256": trade_hash,
        "computed_sha256": trade_hash,
        "raw_path": str(trade_path.resolve()),
        "original_retrieval_timestamp": "2026-01-01T00:00:00+00:00",
        "earliest_trade_timestamp": "2020-01-01T00:00:01+00:00",
        "parser_version": "test",
        "evidence_status": "checksum_verified_official_binance_futures_trade",
    }
    exchange_payload = {
        "symbols": [
            {
                "symbol": "AAAUSDT",
                "baseAsset": "AAA",
                "quoteAsset": "USDT",
                "marginAsset": "USDT",
                "contractType": "PERPETUAL",
                "underlyingType": "COIN",
                "underlyingSubType": ["Layer-1"],
                "onboardDate": 1577836800000,
                "status": "TRADING",
            }
        ]
    }
    exchange_path = raw_dir / "exchangeInfo.json"
    exchange_path.write_text(json.dumps(exchange_payload))

    def write_candidate_index(name: str, prefix: str) -> Path:
        payload = (
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            "<KeyCount>1</KeyCount>"
            f"<CommonPrefixes><Prefix>{prefix}AAAUSDT/</Prefix></CommonPrefixes>"
            "<IsTruncated>false</IsTruncated></ListBucketResult>"
        ).encode()
        digest = hashlib.sha256(payload).hexdigest()
        result = raw_dir / f"{name}_page_001_{digest[:16]}.xml"
        result.write_bytes(payload)
        record_new_snapshot_provenance(
            result,
            url=(
                f"{INDEX_HOST}?list-type=2&prefix={prefix}"
                "&delimiter=/"
            ),
            parser_version="fixture-index-v1",
            retrieved_at="2026-01-01T00:00:00+00:00",
        )
        return result

    monthly_candidate_path = write_candidate_index(
        "monthly_candidates", "data/futures/um/monthly/klines/"
    )
    frontier_candidate_path = write_candidate_index(
        "frontier_candidates", "data/futures/um/daily/klines/"
    )

    def write_contents_index(name: str, prefix: str, key: str) -> Path:
        payload = (
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            "<KeyCount>1</KeyCount>"
            f"<Contents><Key>{key}</Key></Contents>"
            "<IsTruncated>false</IsTruncated></ListBucketResult>"
        ).encode()
        digest = hashlib.sha256(payload).hexdigest()
        result = raw_dir / f"{name}_page_001_{digest[:16]}.xml"
        result.write_bytes(payload)
        record_new_snapshot_provenance(
            result,
            url=f"{INDEX_HOST}?list-type=2&prefix={prefix}",
            parser_version="fixture-index-v1",
            retrieved_at="2026-01-01T00:00:00+00:00",
        )
        return result

    monthly_key = trade["archive_object_key"].replace(
        "daily/trades/AAAUSDT/AAAUSDT-trades-2020-01-01.zip",
        "monthly/klines/AAAUSDT/1h/AAAUSDT-1h-2020-01.zip",
    )
    monthly_observation_path = write_contents_index(
        "monthly_observations",
        "data/futures/um/monthly/klines/AAAUSDT/1h/",
        monthly_key,
    )
    boundary_path = write_contents_index(
        "AAAUSDT",
        "data/futures/um/daily/trades/AAAUSDT/",
        trade["archive_object_key"],
    )

    listing_detail_path = raw_dir / "article_listing_fixture.json"
    listing_detail_path.write_text(
        json.dumps(
            {
                "code": "000000",
                "data": {
                    "title": "Binance Futures Will Launch AAAUSDT Perpetual Contract",
                    "body": json.dumps(
                        {
                            "child": [
                                {
                                    "text": "Binance Futures will launch AAAUSDT at "
                                    "2020-01-01 00:00 (UTC)."
                                }
                            ]
                        }
                    ),
                },
            }
        )
    )
    listing_catalog_path = raw_dir / "catalog_48_page_001_fixture.json"
    listing_catalog_path.write_text(
        json.dumps(
            {
                "code": "000000",
                "data": {
                    "catalogs": [
                        {
                            "catalogId": 48,
                            "total": 1,
                            "articles": [
                                {
                                    "code": "listing",
                                    "title": "Binance Futures Will Launch AAAUSDT Perpetual Contract",
                                    "releaseDate": 1577750400000,
                                }
                            ],
                        }
                    ]
                },
            }
        )
    )
    delisting_catalog_path = raw_dir / "catalog_161_page_001_fixture.json"
    delisting_catalog_path.write_text(
        json.dumps(
            {
                "code": "000000",
                "data": {
                    "catalogs": [
                        {
                            "catalogId": 161,
                            "total": 0,
                            "articles": [],
                        }
                    ]
                },
            }
        )
    )
    listing_evidence = asdict(
        parse_announcement_evidence(
            json.loads(listing_detail_path.read_text()),
            {
                "code": "listing",
                "title": "Binance Futures Will Launch AAAUSDT Perpetual Contract",
                "releaseDate": 1577750400000,
            },
            "listing",
            {"AAAUSDT"},
            retrieved_at="2026-01-01T00:00:00+00:00",
            raw_snapshot_path=str(listing_detail_path.resolve()),
            raw_snapshot_sha256=sha256_path(listing_detail_path),
        )[0]
    )
    announcement_audit = {
        "rebuild_started_at": "2026-01-01T00:00:00+00:00",
        "parser_version": "binance-announcement-semantic-v3",
        "catalogs": [
            {
                "catalog_id": 48,
                "event_type": "listing",
                "inspection_policy": "strict_positive_listing_title_prefilter",
                "candidate_articles": 1,
                "declared_total": 1,
                "pages": 1,
                "page_sha256s": [sha256_path(listing_catalog_path)],
                "detail_sha256s": [
                    {"article_code": "listing", "sha256": sha256_path(listing_detail_path)}
                ],
            },
            {
                "catalog_id": 161,
                "event_type": "delisting",
                "inspection_policy": "complete_catalog_detail_inspection",
                "candidate_articles": 0,
                "declared_total": 0,
                "pages": 1,
                "page_sha256s": [sha256_path(delisting_catalog_path)],
                "detail_sha256s": [],
            },
        ],
    }
    (tmp_path / "announcement_corpus_audit.json").write_text(
        json.dumps(announcement_audit)
    )
    archive = {
        **_archive("AAAUSDT"),
        "archive_raw_snapshot_paths": [str(monthly_observation_path.resolve())],
        "archive_raw_snapshot_sha256s": [sha256_path(monthly_observation_path)],
        "archive_parser_version": "fixture-index-v1",
    }
    exchange_frame = records_from_exchange_info(
        exchange_payload,
        pd.Timestamp("2026-01-01T00:00:00Z"),
        raw_snapshot_path=str(exchange_path.resolve()),
        raw_snapshot_sha256=sha256_path(exchange_path),
    )
    cutoff = {
        "symbol": "AAAUSDT",
        "lifecycle_episode_id": "AAAUSDT:1",
        "article_code": None,
        "official_article_url": None,
        "raw_article_sha256": None,
        "official_publication_timestamp": None,
        "terminal_last_trading_at": None,
        "product_event_disposition": None,
        "review_status": "not_applicable_current_episode",
        "evidence_summary": "Current episode.",
    }
    catalog = build_lifecycle_catalog(
        pd.DataFrame([archive]),
        exchange_frame,
        pd.DataFrame([listing_evidence]),
        announcement_search_completed=True,
        first_observed_trades=pd.DataFrame([trade]),
        delisting_registry_records=[cutoff],
    )
    records = catalog.to_dict("records")
    (tmp_path / "lifecycle_catalog.json").write_text(json.dumps(records, default=str))
    classification = [
        {
            "contract_identity": "AAAUSDT",
            "dimension": dimension,
            "value": False,
            "evidence_status": "accepted_reviewed_evidence",
            "conflict_status": "none",
        }
        for dimension in ("stablecoin_underlying", "leveraged_token")
    ]
    (tmp_path / "classification_evidence.json").write_text(json.dumps(classification))
    (tmp_path / "announcement_evidence.json").write_text(json.dumps([listing_evidence]))
    (tmp_path / "archive_observations.json").write_text(json.dumps([archive]))
    queue = {"status": "reviewed_finite_universe", "prefixes": [], "dispositions": {}}
    (tmp_path / "noncanonical_archive_prefix_queue.json").write_text(json.dumps(queue))
    readiness = catalog_readiness(catalog, [])
    (tmp_path / "readiness.json").write_text(json.dumps(readiness))
    coverage = catalog_coverage(catalog)
    coverage["recomputed_readiness"] = readiness
    (tmp_path / "coverage.json").write_text(json.dumps(coverage))
    registry = build_reviewed_scope_registry_fixture(
        ["AAAUSDT"],
        exchange_payload,
        audited_at="2026-01-01T00:00:00+00:00",
    )
    scope_review_core = {
        "schema_version": SCOPE_REVIEW_SCHEMA_VERSION,
        "verdict": "PASS",
        "candidate_set_digest": registry["candidate_set_digest"],
        "scope_registry_payload_id": registry["registry_payload_id"],
    }
    scope_review = {
        **scope_review_core,
        "review_id": content_identity(scope_review_core),
    }
    scope_review_path = review_dir / "scope-review.json"
    scope_review_path.write_text(json.dumps(scope_review))
    registry["independent_review"] = {
        "identifier": scope_review["review_id"],
        "path": "docs/reviews/scope-review.json",
        "sha256": sha256_path(scope_review_path),
        "verdict": "PASS",
    }
    registry["registry_id"] = scope_registry_identity(registry)
    (tmp_path / "historical_scope_registry.json").write_text(json.dumps(registry))
    (tmp_path / "first_observed_trades.json").write_text(json.dumps([trade]))
    catalog = build_lifecycle_catalog(
        pd.DataFrame([archive]),
        exchange_frame,
        pd.DataFrame([listing_evidence]),
        created_at=pd.Timestamp(records[0]["catalog_created_at"]),
        announcement_search_completed=True,
        first_observed_trades=pd.DataFrame([trade]),
        delisting_registry_records=[cutoff],
        scope_registry_records=registry["records"],
    )
    records = catalog.to_dict("records")
    (tmp_path / "lifecycle_catalog.json").write_text(json.dumps(records, default=str))
    readiness = catalog_readiness(catalog, [])
    (tmp_path / "readiness.json").write_text(json.dumps(readiness))
    coverage = catalog_coverage(catalog)
    coverage["recomputed_readiness"] = readiness
    (tmp_path / "coverage.json").write_text(json.dumps(coverage))
    inventory = {
        "schema_version": "lifecycle-candidate-inventory-v1",
        "candidate_set_digest": registry["candidate_set_digest"],
        "candidate_count": 1,
        "candidate_identities": ["AAAUSDT"],
        "discovered_at": "2026-01-01T00:00:00+00:00",
        "source_identifier": "fixture",
        "discovery_layers": [
            {
                "layer": "historical_monthly_candidates",
                "source_prefix": "data/futures/um/monthly/klines/",
                "candidate_identities": ["AAAUSDT"],
                "raw_snapshot_paths": [str(monthly_candidate_path.resolve())],
                "raw_snapshot_sha256s": [sha256_path(monthly_candidate_path)],
                "retrieval_provenance": "immutable_raw_snapshot_sidecars",
                "source_urls": [
                    f"{INDEX_HOST}?list-type=2&prefix=data/futures/um/monthly/klines/&delimiter=/"
                ],
                "index_audit": {
                    "page_count": 1,
                    "returned_prefix_count": 1,
                    "returned_key_count": 0,
                    "unique_prefix_count": 1,
                    "unique_key_count": 0,
                    "any_page_truncated": False,
                    "source_urls": [
                        f"{INDEX_HOST}?list-type=2&prefix=data/futures/um/monthly/klines/&delimiter=/"
                    ],
                },
            },
            {
                "layer": "frontier_daily_candidates",
                "source_prefix": "data/futures/um/daily/klines/",
                "candidate_identities": ["AAAUSDT"],
                "raw_snapshot_paths": [str(frontier_candidate_path.resolve())],
                "raw_snapshot_sha256s": [sha256_path(frontier_candidate_path)],
                "retrieval_provenance": "immutable_raw_snapshot_sidecars",
                "source_urls": [
                    f"{INDEX_HOST}?list-type=2&prefix=data/futures/um/daily/klines/&delimiter=/"
                ],
                "index_audit": {
                    "page_count": 1,
                    "returned_prefix_count": 1,
                    "returned_key_count": 0,
                    "unique_prefix_count": 1,
                    "unique_key_count": 0,
                    "any_page_truncated": False,
                    "source_urls": [
                        f"{INDEX_HOST}?list-type=2&prefix=data/futures/um/daily/klines/&delimiter=/"
                    ],
                },
            },
        ],
    }
    inventory["inventory_id"] = content_identity(
        {key: value for key, value in inventory.items() if key != "inventory_id"}
    )
    (tmp_path / "candidate_inventory.json").write_text(json.dumps(inventory))
    adjudication_core = {
        "schema_version": "lifecycle-adjudications-v1",
        "candidate_set_digest": registry["candidate_set_digest"],
        "records": [],
    }
    (tmp_path / "lifecycle_adjudications.json").write_text(
        json.dumps(
            {
                **adjudication_core,
                "adjudication_id": content_identity(adjudication_core),
            }
        )
    )

    boundary = {
        "schema_version": "lifecycle-daily-trade-boundaries-v1",
        "symbol": "AAAUSDT",
        "source_identifier": "data/futures/um/daily/trades/AAAUSDT/",
        "raw_snapshot_paths": [str(boundary_path.resolve())],
        "raw_snapshot_sha256s": [sha256_path(boundary_path)],
        "observed_archive_dates": ["2020-01-01"],
        "observed_archive_keys": [trade["archive_object_key"]],
        "observed_daily_trade_dates": ["2020-01-01"],
        "parser_version": "fixture-index-v1",
    }
    (tmp_path / "lifecycle_daily_trade_boundaries.json").write_text(
        json.dumps([boundary])
    )
    episode_core = {
        "schema_version": "lifecycle-episode-first-trade-evidence-v1",
        "candidate_set_digest": registry["candidate_set_digest"],
        "records": [{**trade, "lifecycle_episode_id": "AAAUSDT:1"}],
    }
    (tmp_path / "episode_first_observed_trades.json").write_text(
        json.dumps({**episode_core, "evidence_id": content_identity(episode_core)})
    )
    delisting_core = {
        "schema_version": "historical-delisting-cutoff-registry-v1",
        "candidate_set_digest": registry["candidate_set_digest"],
        "official_cms_corpus": cms_corpus_binding(announcement_audit),
        "reviewed_contract_identities": ["AAAUSDT"],
        "review_version": "fixture-v1",
        "records": [cutoff],
    }
    delisting_payload = {
        **delisting_core,
        "registry_id": content_identity(delisting_core),
    }
    delisting_path = tmp_path / "historical_delisting_cutoff_registry.json"
    delisting_path.write_text(json.dumps(delisting_payload))
    delisting_review_core = {
        "schema_version": "historical-delisting-cutoff-review-v1",
        "verdict": "PASS",
        "registry_id": delisting_payload["registry_id"],
        "registry_sha256": sha256_path(delisting_path),
        "reviewed_episode_count": 1,
    }
    (tmp_path / "delisting_registry_independent_review.json").write_text(
        json.dumps(
            {
                **delisting_review_core,
                "review_id": content_identity(delisting_review_core),
            }
        )
    )
    server_path = raw_dir / "binance_futures_server_time.json"
    server_path.write_text(json.dumps({"serverTime": 1767312000000}))
    for snapshot, url, parser in (
        (exchange_path, "https://official.example/exchangeInfo", "fixture-exchange-v1"),
        (listing_catalog_path, "https://official.example/catalog/listing", "fixture-cms-v1"),
        (delisting_catalog_path, "https://official.example/catalog/delisting", "fixture-cms-v1"),
        (listing_detail_path, "https://official.example/article/listing", "fixture-cms-v1"),
        (server_path, "https://fapi.binance.com/fapi/v1/time", "fixture-server-time-v1"),
    ):
        record_new_snapshot_provenance(
            snapshot,
            url=url,
            parser_version=parser,
            retrieved_at="2026-01-01T00:00:00+00:00",
        )
    primitive_core = {
        "schema_version": "lifecycle-primitive-evidence-manifest-v1",
        "raw_root": str(raw_dir.resolve()),
        "entries": [
            {
                "evidence_role": "archive_candidate_index_xml",
                "path": str(monthly_candidate_path.resolve()),
                "sha256": sha256_path(monthly_candidate_path),
                "source_url": f"{INDEX_HOST}?list-type=2&prefix=data/futures/um/monthly/klines/&delimiter=/",
                "source_identifier": "data/futures/um/monthly/klines/",
                "original_retrieval_timestamp": "2026-01-01T00:00:00+00:00",
                "parser_schema_version": "fixture-index-v1",
            },
            {
                "evidence_role": "frontier_daily_candidate_index_xml",
                "path": str(frontier_candidate_path.resolve()),
                "sha256": sha256_path(frontier_candidate_path),
                "source_url": f"{INDEX_HOST}?list-type=2&prefix=data/futures/um/daily/klines/&delimiter=/",
                "source_identifier": "data/futures/um/daily/klines/",
                "original_retrieval_timestamp": "2026-01-01T00:00:00+00:00",
                "parser_schema_version": "fixture-index-v1",
            },
            {
                "evidence_role": "archive_observation_index_xml",
                "path": str(monthly_observation_path.resolve()),
                "sha256": sha256_path(monthly_observation_path),
                "source_url": f"{INDEX_HOST}?list-type=2&prefix=data/futures/um/monthly/klines/AAAUSDT/1h/",
                "source_identifier": "data/futures/um/monthly/klines/AAAUSDT/1h/",
                "original_retrieval_timestamp": "2026-01-01T00:00:00+00:00",
                "parser_schema_version": "fixture-index-v1",
            },
            {
                "evidence_role": "exchange_info_snapshot",
                "path": str(exchange_path.resolve()),
                "sha256": sha256_path(exchange_path),
                "source_url": "https://official.example/exchangeInfo",
                "source_identifier": "exchangeInfo",
                "original_retrieval_timestamp": "2026-01-01T00:00:00+00:00",
                "parser_schema_version": "fixture-exchange-v1",
            },
            {
                "evidence_role": "first_observed_trade_zip",
                "path": str(trade_path.resolve()),
                "sha256": trade_hash,
                "source_url": "https://official.example/trade.zip",
                "source_identifier": trade["archive_object_key"],
                "original_retrieval_timestamp": trade["original_retrieval_timestamp"],
                "parser_schema_version": "test",
            },
            {
                "evidence_role": "cms_catalog_response",
                "path": str(listing_catalog_path.resolve()),
                "sha256": sha256_path(listing_catalog_path),
                "source_url": "https://official.example/catalog/listing",
                "source_identifier": "catalog-48-page-1",
                "original_retrieval_timestamp": "2026-01-01T00:00:00+00:00",
                "parser_schema_version": "fixture-cms-v1",
            },
            {
                "evidence_role": "cms_catalog_response",
                "path": str(delisting_catalog_path.resolve()),
                "sha256": sha256_path(delisting_catalog_path),
                "source_url": "https://official.example/catalog/delisting",
                "source_identifier": "catalog-161-page-1",
                "original_retrieval_timestamp": "2026-01-01T00:00:00+00:00",
                "parser_schema_version": "fixture-cms-v1",
            },
            {
                "evidence_role": "cms_article_detail_response",
                "path": str(listing_detail_path.resolve()),
                "sha256": sha256_path(listing_detail_path),
                "source_url": "https://official.example/article/listing",
                "source_identifier": "listing",
                "original_retrieval_timestamp": "2026-01-01T00:00:00+00:00",
                "parser_schema_version": "fixture-cms-v1",
            },
            {
                "evidence_role": "daily_trade_boundary_index_xml",
                "path": str(boundary_path.resolve()),
                "sha256": sha256_path(boundary_path),
                "source_url": f"{INDEX_HOST}?list-type=2&prefix=data/futures/um/daily/trades/AAAUSDT/",
                "source_identifier": "data/futures/um/daily/trades/AAAUSDT/",
                "original_retrieval_timestamp": "2026-01-01T00:00:00+00:00",
                "parser_schema_version": "fixture-index-v1",
            },
            {
                "evidence_role": "binance_server_time_snapshot",
                "path": str(server_path.resolve()),
                "sha256": sha256_path(server_path),
                "source_url": "https://fapi.binance.com/fapi/v1/time",
                "source_identifier": "futures-server-time",
                "original_retrieval_timestamp": "2026-01-01T00:00:00+00:00",
                "parser_schema_version": "fixture-server-time-v1",
            },
        ]
        + [
            {
                "evidence_role": "provenance_sidecar",
                "path": str(sidecar.resolve()),
                "sha256": sha256_path(sidecar),
                "source_url": None,
                "source_identifier": sidecar.name.removesuffix(".provenance.json"),
                "original_retrieval_timestamp": None,
                "parser_schema_version": "raw-source-provenance-v1",
            }
            for sidecar in (
                monthly_candidate_path.with_name(
                    f"{monthly_candidate_path.name}.provenance.json"
                ),
                frontier_candidate_path.with_name(
                    f"{frontier_candidate_path.name}.provenance.json"
                ),
                monthly_observation_path.with_name(
                    f"{monthly_observation_path.name}.provenance.json"
                ),
                boundary_path.with_name(f"{boundary_path.name}.provenance.json"),
                exchange_path.with_name(f"{exchange_path.name}.provenance.json"),
                listing_catalog_path.with_name(
                    f"{listing_catalog_path.name}.provenance.json"
                ),
                delisting_catalog_path.with_name(
                    f"{delisting_catalog_path.name}.provenance.json"
                ),
                listing_detail_path.with_name(
                    f"{listing_detail_path.name}.provenance.json"
                ),
                server_path.with_name(f"{server_path.name}.provenance.json"),
            )
        ],
    }
    (tmp_path / "primitive_evidence_manifest.json").write_text(
        json.dumps({**primitive_core, "manifest_id": content_identity(primitive_core)})
    )
    freshness = build_lifecycle_freshness(
        required_valid_through_utc="2020-01-02T00:00:00Z",
        candidate_valid_through_utc="2026-01-01T00:00:00Z",
        episode_valid_through_utc="2026-01-01T00:00:00Z",
        delisting_valid_through_utc="2026-01-01T00:00:00Z",
        server_time_evidence={
            "path": str(server_path.resolve()),
            "sha256": sha256_path(server_path),
            "source_url": "https://fapi.binance.com/fapi/v1/time",
            "server_time_utc": "2026-01-02T00:00:00Z",
            "server_date_utc": "2026-01-02",
            "maximum_archive_backed_horizon_utc": "2026-01-01T00:00:00Z",
        },
        frontier_candidate_discovery={
            "layers": inventory["discovery_layers"],
            "candidate_set_digest": registry["candidate_set_digest"],
        },
        episode_freshness_evidence={
            "schema_version": "lifecycle-episode-freshness-v1",
            "required_valid_through_utc": "2020-01-02T00:00:00Z",
            "symbols": [boundary],
        },
        announcement_corpus={
            "identity": cms_corpus_binding(announcement_audit)["identity"],
            "sha256": cms_corpus_binding(announcement_audit)["sha256"],
            "acquisition_started_at_utc": "2026-01-01T00:00:00Z",
        },
        bound_raw_evidence=[
            {"path": str(server_path.resolve()), "sha256": sha256_path(server_path)},
            {"path": str(trade_path.resolve()), "sha256": trade_hash},
        ],
    )
    (tmp_path / "lifecycle_freshness.json").write_text(json.dumps(freshness))
    config = tmp_path / "config" / "research_v0_1.yaml"
    config.write_text(
        "version: scanner-v0.1\n"
        "universe:\n"
        "  stablecoin_underlyings: [USDT, USTC]\n"
        "data:\n"
        "  start: '2020-01-01T00:00:00Z'\n"
        "  archive_index_url: 'https://official.example/index'\n"
    )
    names = [
        "lifecycle_catalog.json",
        "coverage.json",
        "classification_evidence.json",
        "announcement_evidence.json",
        "archive_observations.json",
        "noncanonical_archive_prefix_queue.json",
        "readiness.json",
        "historical_scope_registry.json",
        "first_observed_trades.json",
        "announcement_corpus_audit.json",
        "candidate_inventory.json",
        "lifecycle_adjudications.json",
        "lifecycle_daily_trade_boundaries.json",
        "primitive_evidence_manifest.json",
        "episode_first_observed_trades.json",
        "lifecycle_freshness.json",
        "historical_delisting_cutoff_registry.json",
        "delisting_registry_independent_review.json",
        "independent_eligibility_verification_report.json",
    ]
    replay = run_eligibility_oracle(
        tmp_path,
        repository_root=tmp_path,
        config_path=config,
        executable_commit="a" * 40,
    )
    (tmp_path / "independent_eligibility_verification_report.json").write_text(
        json.dumps(replay)
    )
    bundle = build_bundle_payload(
        report_root=tmp_path,
        config_path=config,
        artifact_names=names,
        code_commit="a" * 40,
        created_at="2026-01-01T00:00:00+00:00",
    )
    bundle_path = tmp_path / "lifecycle_bundle.json"
    bundle_path.write_text(json.dumps(bundle))
    return bundle_path, catalog


def _resign_primitive_manifest(tmp_path: Path, mutate) -> Path:
    path = tmp_path / "primitive_evidence_manifest.json"
    manifest = json.loads(path.read_text())
    mutate(manifest)
    core = {key: value for key, value in manifest.items() if key != "manifest_id"}
    manifest["manifest_id"] = content_identity(core)
    path.write_text(json.dumps(manifest))
    return path


def _fixture_full_replay(tmp_path: Path) -> dict:
    adjudications = json.loads(
        (tmp_path / "lifecycle_adjudications.json").read_text()
    )
    adjudications["by_symbol"] = {}
    return evidence_replay.run_full_evidence_replay(
        tmp_path,
        repository_root=tmp_path,
        lifecycle_adjudications=adjudications,
    )


def _resign_artifact_and_bundle(bundle_path: Path, artifact_name: str) -> None:
    bundle = json.loads(bundle_path.read_text())
    artifact_path = bundle_path.parent / artifact_name
    artifact_sha256 = sha256_path(artifact_path)
    bundle["artifacts"][artifact_name]["sha256"] = artifact_sha256
    if artifact_name == "lifecycle_freshness.json":
        freshness = json.loads(artifact_path.read_text())
        bundle["authorization_chain"]["lifecycle_freshness_id"] = freshness[
            "freshness_id"
        ]
        bundle["authorization_chain"]["lifecycle_freshness_sha256"] = artifact_sha256
    bundle_core = {key: value for key, value in bundle.items() if key != "bundle_id"}
    bundle["bundle_id"] = content_identity(bundle_core)
    bundle_path.write_text(json.dumps(bundle))


def _tamper_inventory(
    tmp_path: Path, mutate: Callable[[dict], None], *, resign_bundle: bool = False
) -> Path:
    path = tmp_path / "candidate_inventory.json"
    inventory = json.loads(path.read_text())
    mutate(inventory)
    inventory_core = {key: value for key, value in inventory.items() if key != "inventory_id"}
    inventory["inventory_id"] = content_identity(inventory_core)
    path.write_text(json.dumps(inventory))
    if resign_bundle:
        _resign_artifact_and_bundle(tmp_path / "lifecycle_bundle.json", path.name)
    return path


def _tamper_freshness(
    tmp_path: Path, mutate: Callable[[dict], None], *, resign_bundle: bool = False
) -> Path:
    path = tmp_path / "lifecycle_freshness.json"
    freshness = json.loads(path.read_text())
    mutate(freshness)
    freshness_core = {key: value for key, value in freshness.items() if key != "freshness_id"}
    freshness["freshness_id"] = content_identity(freshness_core)
    path.write_text(json.dumps(freshness))
    if resign_bundle:
        _resign_artifact_and_bundle(tmp_path / "lifecycle_bundle.json", path.name)
    return path


def _remove_candidate_layer_from_manifest(tmp_path: Path, role: str) -> None:
    manifest = json.loads((tmp_path / "primitive_evidence_manifest.json").read_text())
    removed_paths = {
        entry["path"] for entry in manifest["entries"] if entry["evidence_role"] == role
    }
    removed_sidecars = {f"{path}.provenance.json" for path in removed_paths}
    for path in removed_paths:
        Path(path).unlink()
        Path(f"{path}.provenance.json").unlink()
    manifest["entries"] = [
        entry
        for entry in manifest["entries"]
        if entry["path"] not in removed_paths | removed_sidecars
    ]
    core = {key: value for key, value in manifest.items() if key != "manifest_id"}
    manifest["manifest_id"] = content_identity(core)
    (tmp_path / "primitive_evidence_manifest.json").write_text(json.dumps(manifest))


@pytest.mark.parametrize(
    "role",
    ["archive_candidate_index_xml", "frontier_daily_candidate_index_xml"],
)
def test_full_replay_requires_actual_primitive_evidence_for_each_candidate_layer(
    tmp_path: Path, role: str
) -> None:
    _write_bundle(tmp_path)
    _remove_candidate_layer_from_manifest(tmp_path, role)
    with pytest.raises(ValueError, match="lacks .*candidates evidence"):
        _fixture_full_replay(tmp_path)


@pytest.mark.parametrize(
    ("layer_index", "field"),
    [
        (0, "candidate_identities"),
        (1, "candidate_identities"),
        (0, "source_prefix"),
        (1, "source_urls"),
        (0, "retrieval_provenance"),
        (1, "index_audit"),
        (0, "raw_snapshot_paths"),
        (1, "raw_snapshot_sha256s"),
    ],
)
def test_full_replay_rejects_every_derived_discovery_layer_claim(
    tmp_path: Path, layer_index: int, field: str
) -> None:
    _write_bundle(tmp_path)

    def mutate(inventory: dict) -> None:
        layer = inventory["discovery_layers"][layer_index]
        if field == "candidate_identities":
            layer[field] = []
        elif field == "source_prefix":
            layer[field] = "data/futures/um/daily/klines/"
        elif field == "source_urls":
            layer[field] = ["https://example.invalid/tampered"]
        elif field == "retrieval_provenance":
            layer[field] = "tampered"
        elif field == "index_audit":
            layer[field]["page_count"] = 99
        elif field == "raw_snapshot_paths":
            layer[field] = inventory["discovery_layers"][1][field]
        else:
            layer[field] = ["0" * 64]

    _tamper_inventory(tmp_path, mutate)
    with pytest.raises(ValueError, match="Candidate inventory discovery layers"):
        _fixture_full_replay(tmp_path)


def test_full_replay_rejects_cross_swapped_discovery_layers(tmp_path: Path) -> None:
    _write_bundle(tmp_path)
    _tamper_inventory(
        tmp_path,
        lambda inventory: inventory["discovery_layers"].reverse(),
    )
    with pytest.raises(ValueError, match="Candidate inventory discovery layers"):
        _fixture_full_replay(tmp_path)


def test_full_replay_rejects_missing_stored_discovery_layer(tmp_path: Path) -> None:
    _write_bundle(tmp_path)
    _tamper_inventory(
        tmp_path,
        lambda inventory: inventory["discovery_layers"].pop(),
    )
    with pytest.raises(ValueError, match="Candidate inventory discovery layers"):
        _fixture_full_replay(tmp_path)


def test_replayed_discovery_layers_accept_real_empty_results(tmp_path: Path) -> None:
    monthly_prefix = "data/futures/um/monthly/klines/"
    frontier_prefix = "data/futures/um/daily/klines/"
    monthly_pages, monthly_urls = _write_index_chain(
        tmp_path,
        prefix=monthly_prefix,
        name="monthly-empty",
        returned_tokens=[],
        delimiter="/",
    )
    frontier_pages, frontier_urls = _write_index_chain(
        tmp_path,
        prefix=frontier_prefix,
        name="frontier-empty",
        returned_tokens=[],
        delimiter="/",
    )
    entries = [
        {
            "evidence_role": "archive_candidate_index_xml",
            "path": str(monthly_pages[0].resolve()),
            "sha256": sha256_path(monthly_pages[0]),
            "source_url": monthly_urls[str(monthly_pages[0].resolve())],
        },
        {
            "evidence_role": "frontier_daily_candidate_index_xml",
            "path": str(frontier_pages[0].resolve()),
            "sha256": sha256_path(frontier_pages[0]),
            "source_url": frontier_urls[str(frontier_pages[0].resolve())],
        },
    ]
    layers, candidates = evidence_replay._replay_discovery_layers(entries)
    assert candidates == []
    assert [layer["candidate_identities"] for layer in layers] == [[], []]


@pytest.mark.parametrize("freshness_field", ["layers", "candidate_set_digest"])
def test_full_replay_rejects_tampered_frontier_candidate_discovery(
    tmp_path: Path, freshness_field: str
) -> None:
    _write_bundle(tmp_path)

    def mutate(freshness: dict) -> None:
        if freshness_field == "layers":
            freshness["frontier_candidate_discovery"]["layers"] = []
        else:
            freshness["frontier_candidate_discovery"][freshness_field] = "0" * 64

    _tamper_freshness(tmp_path, mutate)
    with pytest.raises(ValueError, match="Freshness frontier candidate discovery"):
        _fixture_full_replay(tmp_path)


def test_verify_lifecycle_bundle_rejects_derived_metadata_attacks_via_full_replay(
    tmp_path: Path,
) -> None:
    bundle_path, _ = _write_bundle(tmp_path)
    _tamper_inventory(
        tmp_path,
        lambda inventory: inventory["discovery_layers"][0].update(
            {"candidate_identities": []}
        ),
        resign_bundle=True,
    )
    with pytest.raises(ValueError, match="Candidate inventory discovery layers"):
        verify_lifecycle_bundle(bundle_path)

    freshness_root = tmp_path / "freshness-attack"
    freshness_root.mkdir()
    freshness_bundle, _ = _write_bundle(freshness_root)
    _tamper_freshness(
        freshness_root,
        lambda freshness: freshness["frontier_candidate_discovery"].update(
            {"candidate_set_digest": "0" * 64}
        ),
        resign_bundle=True,
    )
    with pytest.raises(ValueError, match="Freshness frontier candidate discovery"):
        verify_lifecycle_bundle(freshness_bundle)


def test_candidate_layer_with_manifest_bound_empty_result_is_valid(tmp_path: Path) -> None:
    monthly_prefix = "data/futures/um/monthly/klines/"
    frontier_prefix = "data/futures/um/daily/klines/"
    monthly_pages, monthly_urls = _write_index_chain(
        tmp_path,
        prefix=monthly_prefix,
        name="monthly",
        returned_tokens=[],
        delimiter="/",
        entries_on_terminal_page=[f"{monthly_prefix}AAAUSDT/"],
        entries_are_prefixes=True,
    )
    frontier_pages, frontier_urls = _write_index_chain(
        tmp_path,
        prefix=frontier_prefix,
        name="frontier",
        returned_tokens=[],
        delimiter="/",
    )
    entries = [
        {
            "evidence_role": "archive_candidate_index_xml",
            "path": str(monthly_pages[0].resolve()),
            "source_url": monthly_urls[str(monthly_pages[0].resolve())],
        },
        {
            "evidence_role": "frontier_daily_candidate_index_xml",
            "path": str(frontier_pages[0].resolve()),
            "source_url": frontier_urls[str(frontier_pages[0].resolve())],
        },
    ]
    assert evidence_replay._candidate_identities(entries) == (["AAAUSDT"], [])


@pytest.mark.parametrize("returned_tokens", [["A", "B", "B"], ["A", "B", "A"]])
def test_candidate_replay_rejects_repeated_continuation_tokens(
    tmp_path: Path, returned_tokens: list[str]
) -> None:
    prefix = "data/futures/um/monthly/klines/"
    pages, urls = _write_index_chain(
        tmp_path,
        prefix=prefix,
        name="candidate",
        returned_tokens=returned_tokens,
        delimiter="/",
        entries_on_terminal_page=[f"{prefix}AAAUSDT/"],
        entries_are_prefixes=True,
    )
    entries = [
        {
            "evidence_role": "archive_candidate_index_xml",
            "path": str(path.resolve()),
            "source_url": urls[str(path.resolve())],
        }
        for path in pages
    ]
    with pytest.raises(ValueError, match="repeat a continuation token"):
        evidence_replay._replay_s3_index_pages(
            [entry["path"] for entry in entries],
            prefix,
            expected_urls=urls,
        )


def test_candidate_replay_accepts_unique_multi_page_chain(tmp_path: Path) -> None:
    prefix = "data/futures/um/monthly/klines/"
    pages, urls = _write_index_chain(
        tmp_path,
        prefix=prefix,
        name="candidate",
        returned_tokens=["A", "B"],
        delimiter="/",
        entries_on_terminal_page=[f"{prefix}AAAUSDT/"],
        entries_are_prefixes=True,
    )
    prefixes, keys = evidence_replay._replay_s3_index_pages(
        [str(path.resolve()) for path in pages],
        prefix,
        expected_urls=urls,
    )
    assert prefixes == [f"{prefix}AAAUSDT/"]
    assert keys == []


@pytest.mark.parametrize("returned_tokens", [["A", "B", "B"], ["A", "B", "A"]])
def test_monthly_replay_rejects_repeated_continuation_tokens(
    tmp_path: Path, returned_tokens: list[str]
) -> None:
    prefix = "data/futures/um/monthly/klines/AAAUSDT/1h/"
    key = f"{prefix}AAAUSDT-1h-2020-01.zip"
    pages, _urls = _write_index_chain(
        tmp_path,
        prefix=prefix,
        name="monthly",
        returned_tokens=returned_tokens,
        entries_on_terminal_page=[key],
    )
    with pytest.raises(ValueError, match="repeat a continuation token"):
        public_data.observed_zip_keys_from_index_snapshots(
            [str(path.resolve()) for path in pages],
            "AAAUSDT",
        )


def test_monthly_replay_accepts_unique_multi_page_chain(tmp_path: Path) -> None:
    prefix = "data/futures/um/monthly/klines/AAAUSDT/1h/"
    key = f"{prefix}AAAUSDT-1h-2020-01.zip"
    pages, _urls = _write_index_chain(
        tmp_path,
        prefix=prefix,
        name="monthly",
        returned_tokens=["A", "B"],
        entries_on_terminal_page=[key],
    )
    assert public_data.observed_zip_keys_from_index_snapshots(
        [str(path.resolve()) for path in pages],
        "AAAUSDT",
    ) == (key,)


def test_daily_trade_replay_accepts_reserved_character_continuation_token(
    tmp_path: Path,
) -> None:
    prefix = "data/futures/um/daily/trades/AAAUSDT/"
    key = f"{prefix}AAAUSDT-trades-2020-01-01.zip"
    pages, urls = _write_index_chain(
        tmp_path,
        prefix=prefix,
        name="daily",
        returned_tokens=["A/B+C"],
        entries_on_terminal_page=[key],
    )
    assert public_data.observed_daily_trade_keys_from_index_snapshots(
        [str(path.resolve()) for path in pages],
        "AAAUSDT",
    ) == (key,)
    assert "continuation-token=A%2FB%2BC" in urls[str(pages[1].resolve())]


def test_daily_trade_replay_rejects_repeated_continuation_tokens(tmp_path: Path) -> None:
    prefix = "data/futures/um/daily/trades/AAAUSDT/"
    key = f"{prefix}AAAUSDT-trades-2020-01-01.zip"
    pages, _urls = _write_index_chain(
        tmp_path,
        prefix=prefix,
        name="daily",
        returned_tokens=["A", "B", "B"],
        entries_on_terminal_page=[key],
    )
    with pytest.raises(ValueError, match="repeat a continuation token"):
        public_data.observed_daily_trade_keys_from_index_snapshots(
            [str(path.resolve()) for path in pages],
            "AAAUSDT",
        )


@pytest.mark.parametrize(
    ("page_numbers", "page_states", "message"),
    [
        ([1], [(True, None, None)], "no usable continuation token"),
        ([1], [(False, None, "unexpected")], "first page unexpectedly"),
        ([1, 2], [(False, None, None), (False, None, None)], "after a terminal page"),
        ([1, 3], [(False, None, None), (False, None, None)], "missing or reordered page"),
    ],
)
def test_shared_s3_replay_validator_rejects_invalid_chain_shapes(
    page_numbers: list[int],
    page_states: list[tuple[bool, str | None, str | None]],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        public_data._validate_s3_replay_page_chain(
            page_numbers,
            page_states,
            label="Test index",
        )


def test_full_replay_uses_registry_terminal_freshness_detector(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_bundle(tmp_path)
    observed: list[list[dict]] = []
    original = evidence_replay.episode_freshness_review_required

    def wrapped(*args, **kwargs):
        observed.append(kwargs["reviewed_delisting_records"])
        return original(*args, **kwargs)

    monkeypatch.setattr(evidence_replay, "episode_freshness_review_required", wrapped)
    assert _fixture_full_replay(tmp_path)["status"] == "PASS"
    assert observed == [json.loads(
        (tmp_path / "historical_delisting_cutoff_registry.json").read_text()
    )["records"]]


def test_manifest_rejects_omitted_adjacent_replay_sidecar_after_resigning(
    tmp_path: Path,
) -> None:
    _write_bundle(tmp_path)

    def omit_sidecar(manifest: dict) -> None:
        snapshot_path = next(
            entry["path"]
            for entry in manifest["entries"]
            if entry["evidence_role"] == "archive_candidate_index_xml"
        )
        sidecar_path = f"{snapshot_path}.provenance.json"
        manifest["entries"] = [
            entry
            for entry in manifest["entries"]
            if entry["path"] != sidecar_path
        ]

    with pytest.raises(ValueError, match="provenance sidecars"):
        evidence_replay.verify_primitive_manifest(_resign_primitive_manifest(tmp_path, omit_sidecar))


def test_manifest_rejects_wrong_sidecar_sha_after_resigning(tmp_path: Path) -> None:
    _write_bundle(tmp_path)

    def wrong_sha(manifest: dict) -> None:
        next(
            entry
            for entry in manifest["entries"]
            if entry["evidence_role"] == "provenance_sidecar"
        )["sha256"] = "0" * 64

    with pytest.raises(ValueError, match="hash mismatch"):
        evidence_replay.verify_primitive_manifest(_resign_primitive_manifest(tmp_path, wrong_sha))


def test_manifest_rejects_sidecar_url_different_from_snapshot_entry(
    tmp_path: Path,
) -> None:
    _write_bundle(tmp_path)

    def wrong_url(manifest: dict) -> None:
        next(
            entry
            for entry in manifest["entries"]
            if entry["evidence_role"] == "archive_candidate_index_xml"
        )["source_url"] = "https://official.example/forged"

    with pytest.raises(ValueError, match="provenance"):
        evidence_replay.verify_primitive_manifest(_resign_primitive_manifest(tmp_path, wrong_url))


def test_manifest_rejects_sidecar_and_snapshot_paths_outside_raw_root(
    tmp_path: Path,
) -> None:
    sidecar_case = tmp_path / "sidecar-case"
    sidecar_case.mkdir()
    _write_bundle(sidecar_case)
    manifest_path = sidecar_case / "primitive_evidence_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    sidecar = next(
        entry for entry in manifest["entries"] if entry["evidence_role"] == "provenance_sidecar"
    )
    outside_sidecar = sidecar_case / "outside.provenance.json"
    outside_sidecar.write_bytes(Path(sidecar["path"]).read_bytes())
    sidecar["path"] = str(outside_sidecar.resolve())
    core = {key: value for key, value in manifest.items() if key != "manifest_id"}
    manifest["manifest_id"] = content_identity(core)
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="outside raw_root"):
        evidence_replay.verify_primitive_manifest(manifest_path)

    snapshot_case = tmp_path / "snapshot-case"
    snapshot_case.mkdir()
    _write_bundle(snapshot_case)
    manifest_path = snapshot_case / "primitive_evidence_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    snapshot = next(
        entry
        for entry in manifest["entries"]
        if entry["evidence_role"] == "archive_candidate_index_xml"
    )
    outside_snapshot = snapshot_case / "outside.xml"
    outside_snapshot.write_bytes(Path(snapshot["path"]).read_bytes())
    snapshot["path"] = str(outside_snapshot.resolve())
    core = {key: value for key, value in manifest.items() if key != "manifest_id"}
    manifest["manifest_id"] = content_identity(core)
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="outside raw_root"):
        evidence_replay.verify_primitive_manifest(manifest_path)


def test_manifest_and_replay_pass_with_canonical_bound_sidecars(tmp_path: Path) -> None:
    _write_bundle(tmp_path)
    assert evidence_replay.verify_primitive_manifest(
        tmp_path / "primitive_evidence_manifest.json"
    )["entries"]
    assert _fixture_full_replay(tmp_path)["status"] == "PASS"


@pytest.mark.parametrize(
    "url",
    [
        "https://evil.example/data?list-type=2&prefix=data/futures/um/monthly/klines/AAAUSDT/1h/",
        f"{INDEX_HOST}/wrong?list-type=2&prefix=data/futures/um/monthly/klines/AAAUSDT/1h/",
        f"{INDEX_HOST}?prefix=data/futures/um/monthly/klines/AAAUSDT/1h/",
        f"{INDEX_HOST}?list-type=2&prefix=data/futures/um/monthly/klines/AAAUSDT/1h/&extra=x",
        f"{INDEX_HOST}?list-type=2&prefix=data/futures/um/daily/trades/AAAUSDT/&delimiter=/",
    ],
)
def test_replay_rejects_noncanonical_s3_request_identity(url: str) -> None:
    with pytest.raises(ValueError):
        _parse_s3_index_request(
            url,
            "data/futures/um/monthly/klines/AAAUSDT/1h/"
            if "monthly" in url
            else "data/futures/um/daily/trades/AAAUSDT/",
            None,
        )


def test_full_replay_requires_exact_delisting_horizon(tmp_path: Path) -> None:
    _write_bundle(tmp_path)
    freshness_path = tmp_path / "lifecycle_freshness.json"
    freshness = json.loads(freshness_path.read_text())
    freshness["delisting_valid_through_utc"] = "2025-12-31T00:00:00Z"
    freshness["lifecycle_evidence_valid_through_utc"] = "2025-12-31T00:00:00Z"
    core = {key: value for key, value in freshness.items() if key != "freshness_id"}
    freshness["freshness_id"] = content_identity(core)
    freshness_path.write_text(json.dumps(freshness))
    with pytest.raises(ValueError, match="Delisting freshness horizon"):
        _fixture_full_replay(tmp_path)


def test_full_replay_accepts_exact_delisting_horizon(tmp_path: Path) -> None:
    _write_bundle(tmp_path)
    assert _fixture_full_replay(tmp_path)["status"] == "PASS"


def test_freshness_artifact_and_bound_raw_evidence_tampering_fail_bundle_verification(
    tmp_path: Path,
) -> None:
    bundle_path, _ = _write_bundle(tmp_path)
    freshness_path = tmp_path / "lifecycle_freshness.json"
    freshness = json.loads(freshness_path.read_text())
    freshness["candidate_valid_through_utc"] = "2026-01-01T01:00:00Z"
    freshness_path.write_text(json.dumps(freshness))
    with pytest.raises(ValueError, match="artifact hash mismatch"):
        verify_lifecycle_bundle(bundle_path)

    tamper_root = tmp_path / "raw-tamper"
    tamper_root.mkdir()
    bundle_path, _ = _write_bundle(tamper_root)
    server_path = tamper_root / "raw" / "binance_futures_server_time.json"
    server_path.write_text('{"serverTime": 1767312000000} ')  # Same value, different bytes.
    with pytest.raises(ValueError, match="raw evidence hash mismatch"):
        verify_lifecycle_bundle(bundle_path)


def test_verify_lifecycle_bundle_calls_canonical_full_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle_path, _ = _write_bundle(tmp_path)
    calls: list[tuple[Path, Path, str]] = []

    def fail_replay(
        report_root: str | Path,
        *,
        repository_root: str | Path,
        lifecycle_adjudications: dict,
    ) -> dict:
        calls.append(
            (
                Path(report_root),
                Path(repository_root),
                str(lifecycle_adjudications["path"]),
            )
        )
        raise ValueError("canonical replay sentinel")

    monkeypatch.setattr(evidence_replay, "run_full_evidence_replay", fail_replay)
    with pytest.raises(ValueError, match="canonical replay sentinel"):
        verify_lifecycle_bundle(bundle_path)
    assert calls == [(tmp_path.resolve(), tmp_path.resolve(), str((tmp_path / "lifecycle_adjudications.json").resolve()))]


def test_verify_lifecycle_bundle_replay_is_network_free(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle_path, _ = _write_bundle(tmp_path)

    def no_live_network(*args, **kwargs):
        raise AssertionError("live network acquisition is forbidden during verification")

    monkeypatch.setattr(public_data, "_read_url", no_live_network)
    assert verify_lifecycle_bundle(bundle_path)["readiness"]["authorization_ready"] is True


def _write_plan(tmp_path: Path, bundle_path: Path) -> Path:
    verified = verify_lifecycle_bundle(bundle_path)
    bundle = verified["bundle"]
    review_path = tmp_path / "independent-review.md"
    review_path.write_text("PASS")
    state_path = tmp_path / "approval-state.json"
    approval_core = {
        "schema_version": APPROVAL_SCHEMA_VERSION,
        "lifecycle_bundle_id": bundle["bundle_id"],
        "lifecycle_evidence_code_commit": bundle["code_commit"],
        "config_sha256": bundle["config"]["sha256"],
        "independent_review_artifact": {
            "identifier": "fixture-review",
            "path": str(review_path.resolve()),
            "sha256": sha256_path(review_path),
        },
        "approval_purpose": "full_history_acquisition_after_lifecycle_audit",
        "approval_status": "active",
        "approval_state_registry": {"path": str(state_path.resolve())},
        "approved_at": "2026-01-01T00:00:00+00:00",
    }
    approval = {**approval_core, "approval_id": content_identity(approval_core)}
    approval_path = tmp_path / "fixture-approval.json"
    approval_path.write_text(json.dumps(approval))
    state_core = {
        "schema_version": APPROVAL_STATE_SCHEMA_VERSION,
        "approvals": {
            approval["approval_id"]: {
                "status": "active",
                "superseded_by": None,
                "reason": "fixture",
            }
        },
    }
    state_registry = {**state_core, "registry_id": content_identity(state_core)}
    state_path.write_text(json.dumps(state_registry))
    plan = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "created_at": "2026-01-01T00:00:00+00:00",
        "purpose": "authorized_full_history_archive_download",
        "lifecycle_bundle": str(bundle_path.resolve()),
        "lifecycle_bundle_id": bundle["bundle_id"],
        "lifecycle_approval": str(approval_path.resolve()),
        "lifecycle_approval_id": approval["approval_id"],
        "approval_state_registry": {
            "path": str(state_path.resolve()),
            "sha256": sha256_path(state_path),
            "registry_id": state_registry["registry_id"],
        },
        "artifact_hashes": {
            name: descriptor["sha256"] for name, descriptor in bundle["artifacts"].items()
        },
        "config_sha256": bundle["config"]["sha256"],
        "readiness": verified["readiness"],
        "source": "https://official.example/index",
        "planning_basis": "exact_observed_monthly_zip_objects",
        "warmup_start_month": "2019-12",
        "end_month": "2020-01",
        "symbols_discovered": 1,
        "objects": [_archive("AAAUSDT")["observed_archive_object_keys"][0]],
    }
    plan["plan_integrity"] = build_plan_integrity(plan)
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan))
    return path


def test_content_bound_bundle_and_plan_detect_mutation_and_forgery(tmp_path: Path) -> None:
    bundle_path, _ = _write_bundle(tmp_path)
    plan_path = _write_plan(tmp_path, bundle_path)
    assert verify_bound_plan(plan_path)["plan"]["objects"]

    simple_forgery = tmp_path / "simple-forgery.json"
    simple_forgery.write_text(
        json.dumps({"objects": [_archive("AAAUSDT")["observed_archive_object_keys"][0]]})
    )
    with pytest.raises(ValueError, match="unsupported schema"):
        verify_bound_plan(simple_forgery)

    extra_object = json.loads(plan_path.read_text())
    extra_object["objects"].append(
        "data/futures/um/monthly/klines/AAAUSDT/1h/AAAUSDT-1h-2020-02.zip"
    )
    extra_object["plan_integrity"] = build_plan_integrity(extra_object)
    plan_path.write_text(json.dumps(extra_object))
    with pytest.raises(ValueError, match="exact observed approved ZIP objects"):
        verify_bound_plan(plan_path)

    plan_path = _write_plan(tmp_path, bundle_path)
    forged = json.loads(plan_path.read_text())
    forged["lifecycle_bundle_id"] = "0" * 64
    forged["plan_integrity"] = build_plan_integrity(forged)
    plan_path.write_text(json.dumps(forged))
    with pytest.raises(ValueError, match="wrong lifecycle bundle"):
        verify_bound_plan(plan_path)

    plan_path = _write_plan(tmp_path, bundle_path)
    (tmp_path / "coverage.json").write_text("{}")
    with pytest.raises(ValueError, match="artifact hash mismatch"):
        verify_bound_plan(plan_path)


def _two_episode_oracle_fixture() -> tuple[dict, list[dict]]:
    expected = {
        "episodes": [
            {
                "symbol": "AAAUSDT",
                "scope_disposition": "in_scope_crypto_perpetual",
                "lifecycle_episode_id": "AAAUSDT:1",
                "age_live_anchor_at": "2023-01-01T00:00:00+00:00",
                "anchor_basis": "first_observed_binance_futures_trade",
                "eligible_from": "2023-01-31T00:00:00+00:00",
                "delisting_announcement_published_at": "2023-03-01T00:00:00+00:00",
                "delisting_article_id": "article-1",
                "last_trading_at": "2023-03-02T00:00:00+00:00",
                "conflict": None,
            },
            {
                "symbol": "AAAUSDT",
                "scope_disposition": "in_scope_crypto_perpetual",
                "lifecycle_episode_id": "AAAUSDT:2",
                "age_live_anchor_at": "2023-04-01T00:00:00+00:00",
                "anchor_basis": "first_observed_binance_futures_trade",
                "eligible_from": "2023-05-01T00:00:00+00:00",
                "delisting_announcement_published_at": None,
                "delisting_article_id": None,
                "last_trading_at": None,
                "conflict": None,
            },
        ]
    }
    intervals = [
        {
            **{key: value for key, value in item.items() if key not in {"scope_disposition", "conflict"}},
            "interval_evidence_status": "reviewed_resolved",
        }
        for item in expected["episodes"]
    ]
    catalog = [
        {
            "symbol": "AAAUSDT",
            "scope_disposition": "in_scope_crypto_perpetual",
            "lifecycle_intervals": intervals,
            "historical_inclusion_readiness": "ready",
        }
    ]
    return expected, catalog


@pytest.mark.parametrize(
    "mutation",
    [
        "early_anchor",
        "exact_listing_leak",
        "missing_relist_age_reset",
        "gap_continuity",
        "runtime_effective_end",
        "wrong_delisting_article",
        "wrong_delisting_time",
        "wrong_delisting_symbol",
        "wrong_delisting_episode",
        "missing_cutoff",
        "fabricated_cutoff",
        "readiness",
    ],
)
def test_independent_oracle_rejects_production_lifecycle_mutations(mutation: str) -> None:
    expected, catalog = _two_episode_oracle_fixture()
    intervals = catalog[0]["lifecycle_intervals"]
    if mutation == "early_anchor":
        intervals[0]["age_live_anchor_at"] = "2022-12-31T23:00:00+00:00"
    elif mutation == "exact_listing_leak":
        intervals[0]["age_live_anchor_at"] = "2022-12-20T00:00:00+00:00"
        intervals[0]["anchor_basis"] = "exact_official_original_launch"
    elif mutation == "missing_relist_age_reset":
        intervals[1]["eligible_from"] = intervals[1]["age_live_anchor_at"]
    elif mutation == "gap_continuity":
        intervals[0]["last_trading_at"] = None
    elif mutation == "runtime_effective_end":
        intervals[0]["eligibility_end_at"] = intervals[0]["last_trading_at"]
    elif mutation == "wrong_delisting_article":
        intervals[0]["delisting_article_id"] = "wrong-article"
    elif mutation == "wrong_delisting_time":
        intervals[0]["delisting_announcement_published_at"] = "2023-03-01T01:00:00Z"
    elif mutation == "wrong_delisting_symbol":
        intervals[0]["delisting_article_id"] = "article-for-BBBUSDT"
    elif mutation == "wrong_delisting_episode":
        intervals[1]["delisting_article_id"] = "article-1"
    elif mutation == "missing_cutoff":
        intervals[0]["delisting_announcement_published_at"] = None
    elif mutation == "fabricated_cutoff":
        intervals[1]["delisting_announcement_published_at"] = "2024-01-01T00:00:00Z"
    else:
        expected["episodes"][0]["conflict"] = "unresolved_delisting_evidence"
    with pytest.raises(ValueError, match="Oracle"):
        compare_production_catalog(expected, catalog)


def _resign_report_and_bundle(bundle_path: Path, report: dict) -> None:
    report_core = {key: value for key, value in report.items() if key != "verification_report_id"}
    report["verification_report_id"] = content_identity(report_core)
    report_path = bundle_path.parent / "independent_eligibility_verification_report.json"
    report_path.write_text(json.dumps(report))
    bundle = json.loads(bundle_path.read_text())
    digest = sha256_path(report_path)
    bundle["artifacts"]["independent_eligibility_verification_report.json"]["sha256"] = digest
    bundle["authorization_chain"]["eligibility_oracle_report_id"] = report[
        "verification_report_id"
    ]
    bundle["authorization_chain"]["eligibility_oracle_report_sha256"] = digest
    core = {key: value for key, value in bundle.items() if key != "bundle_id"}
    bundle["bundle_id"] = content_identity(core)
    bundle_path.write_text(json.dumps(bundle))


@pytest.mark.parametrize(
    ("field", "wrong"),
    [
        ("candidate_set_digest", "0" * 64),
        ("candidate_count", 2),
        ("reviewed_scope_registry_id", "0" * 64),
        ("scope_independent_review_id", "0" * 64),
        ("reviewed_delisting_registry_id", "0" * 64),
        ("delisting_independent_review_id", "0" * 64),
        ("lifecycle_adjudication_id", "0" * 64),
        ("production_catalog_sha256", "0" * 64),
        ("catalog_row_count", 2),
        ("independently_derived_ready_count", 0),
        ("executable_lifecycle_commit", "b" * 40),
        ("primitive_manifest_id", "0" * 64),
        ("episode_boundary_evidence_id", "0" * 64),
    ],
)
def test_runtime_rejects_content_hashed_but_cross_bundle_wrong_report(
    tmp_path: Path, field: str, wrong: object
) -> None:
    bundle_path, _ = _write_bundle(tmp_path)
    report_path = tmp_path / "independent_eligibility_verification_report.json"
    report = json.loads(report_path.read_text())
    report[field] = wrong
    _resign_report_and_bundle(bundle_path, report)
    with pytest.raises(ValueError, match="authorization chain"):
        verify_lifecycle_bundle(bundle_path)


def test_runtime_rejects_placeholder_pass_report(tmp_path: Path) -> None:
    bundle_path, _ = _write_bundle(tmp_path)
    report = {
        "schema_version": "independent-eligibility-oracle-report-v1",
        "oracle_verification_status": "PASS",
        "final_status": "PASS",
    }
    _resign_report_and_bundle(bundle_path, report)
    with pytest.raises(ValueError, match="authorization chain"):
        verify_lifecycle_bundle(bundle_path)


def test_oracle_module_has_no_production_parser_or_builder_dependency() -> None:
    source = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "alt_hot_scanner"
        / "universe"
        / "eligibility_oracle.py"
    ).read_text("utf-8")
    assert "parse_announcement_evidence" not in source
    assert "build_lifecycle_catalog" not in source


@pytest.mark.parametrize(
    "mutation",
    [
        "wrong_symbol_archive",
        "wrong_manifest_lineage",
        "wrong_manifest_role",
        "wrong_archive_date",
        "wrong_manifest_hash",
    ],
)
def test_oracle_rejects_mislabeled_trade_primitive(
    tmp_path: Path, mutation: str
) -> None:
    _write_bundle(tmp_path)
    episode_path = tmp_path / "episode_first_observed_trades.json"
    episode = json.loads(episode_path.read_text())
    if mutation == "wrong_symbol_archive":
        episode["records"][0]["archive_object_key"] = (
            "data/futures/um/daily/trades/BBBUSDT/BBBUSDT-trades-2020-01-01.zip"
        )
    elif mutation in {
        "wrong_manifest_lineage",
        "wrong_manifest_role",
        "wrong_manifest_hash",
    }:
        primitive_path = tmp_path / "primitive_evidence_manifest.json"
        primitive = json.loads(primitive_path.read_text())
        trade_entry = next(
            entry
            for entry in primitive["entries"]
            if entry["evidence_role"] == "first_observed_trade_zip"
        )
        if mutation == "wrong_manifest_lineage":
            trade_entry["source_identifier"] = "wrong-archive"
        elif mutation == "wrong_manifest_role":
            trade_entry["evidence_role"] = "episode_first_observed_trade_zip"
        else:
            trade_entry["sha256"] = "0" * 64
        primitive_core = {
            key: value for key, value in primitive.items() if key != "manifest_id"
        }
        primitive["manifest_id"] = content_identity(primitive_core)
        primitive_path.write_text(json.dumps(primitive))
    else:
        episode["records"][0]["archive_date"] = "2020-01-02"
    episode_core = {key: value for key, value in episode.items() if key != "evidence_id"}
    episode["evidence_id"] = content_identity(episode_core)
    episode_path.write_text(json.dumps(episode))
    derived = derive_expected_eligibility(tmp_path)
    assert derived["blocker_symbols"] == ["AAAUSDT"]


def test_oracle_rejects_stale_reviewed_cms_corpus(tmp_path: Path) -> None:
    _write_bundle(tmp_path)
    registry_path = tmp_path / "historical_delisting_cutoff_registry.json"
    registry = json.loads(registry_path.read_text())
    registry["official_cms_corpus"]["identity"] = "0" * 64
    registry_core = {key: value for key, value in registry.items() if key != "registry_id"}
    registry["registry_id"] = content_identity(registry_core)
    registry_path.write_text(json.dumps(registry))
    with pytest.raises(ValueError, match="stale CMS corpus"):
        derive_expected_eligibility(tmp_path)
