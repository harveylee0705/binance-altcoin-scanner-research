from __future__ import annotations

import hashlib
import io
import json
import zipfile
from pathlib import Path

import pandas as pd
import pytest

import alt_hot_scanner.data.binance_public as public_data
from alt_hot_scanner.data.announcements import (
    classify_article_semantics,
    parse_announcement_evidence,
)
from alt_hot_scanner.data.binance_public import (
    acquire_first_observed_trade,
    parse_earliest_trade_timestamp,
    validate_daily_trade_object_key,
)
from alt_hot_scanner.data.provenance import record_new_snapshot_provenance
from alt_hot_scanner.universe.authorization import (
    APPROVAL_SCHEMA_VERSION,
    APPROVAL_STATE_SCHEMA_VERSION,
    content_identity,
    sha256_path,
    verify_approval_pin,
)
from alt_hot_scanner.universe.checkpoint import (
    ARCHIVE_CHECKPOINT_SCHEMA_VERSION,
    verify_archive_checkpoint,
)
from alt_hot_scanner.universe.eligibility import apply_point_in_time_eligibility
from alt_hot_scanner.universe.lifecycle import build_lifecycle_catalog
from alt_hot_scanner.universe.scope_registry import (
    candidate_set_digest,
    verify_scope_registry,
)
from tests.scope_registry_fixtures import build_reviewed_scope_registry_fixture

S3_NAMESPACE = "http://s3.amazonaws.com/doc/2006-03-01/"


def _archive(symbol: str = "ABCUSDT") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": symbol,
                "first_archive_month": "2021-01",
                "last_archive_month": "2021-02",
                "archive_discovery_timestamp": "2026-08-23T00:00:00+00:00",
                "archive_source_url": "https://official.example",
                "archive_discovery_provenance": "official_raw_xml",
            }
        ]
    )


def _scope(symbol: str = "ABCUSDT") -> list[dict]:
    return [
        {
            "contract_identity": symbol,
            "base_asset": symbol.removesuffix("USDT"),
            "quote_asset": "USDT",
            "product_scope": "in_scope_crypto_perpetual",
            "is_crypto_underlying": True,
            "is_stablecoin_underlying": False,
            "is_leveraged_token": False,
            "stablecoin_evidence_status": "reviewed_finite_universe_negative",
            "leveraged_evidence_status": "reviewed_finite_universe_negative",
            "scope_audit_status": "complete",
        }
    ]


def _listing(at: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "event_type": "listing",
                "symbol": "ABCUSDT",
                "match_status": "accepted",
                "article_published_at": "2021-01-01T00:00:00+00:00",
                "official_event_at": at,
                "event_time_evidence_status": "action_symbol_time_anchored",
                "source_url": "https://official.example/list",
                "article_code": "launch",
                "retrieved_at": "2026-01-01T00:00:00+00:00",
                "raw_snapshot_path": "/launch.json",
                "raw_snapshot_sha256": "a" * 64,
                "parser_version": "test",
            }
        ]
    )


def _trade(at: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": "ABCUSDT",
                "archive_object_key": (
                    "data/futures/um/daily/trades/ABCUSDT/ABCUSDT-trades-2021-01-01.zip"
                ),
                "published_sha256": "b" * 64,
                "computed_sha256": "b" * 64,
                "raw_path": "/trade.zip",
                "original_retrieval_timestamp": "2026-01-01T00:00:00+00:00",
                "earliest_trade_timestamp": at,
                "parser_version": "test",
                "evidence_status": "checksum_verified_official_binance_futures_trade",
            }
        ]
    )


def test_exact_launch_preferred_and_observed_trade_is_not_relabelled_exact() -> None:
    catalog = build_lifecycle_catalog(
        _archive(),
        pd.DataFrame(),
        _listing("2021-01-01T00:00:00+00:00"),
        first_observed_trades=_trade("2021-01-01T00:00:01+00:00"),
        scope_registry_records=_scope(),
        announcement_search_completed=True,
    )
    row = catalog.iloc[0]
    assert row["eligibility_age_anchor_basis"] == "exact_official_original_launch"
    assert row["exact_official_trading_start_at"] == "2021-01-01T00:00:00+00:00"
    assert row["first_observed_trade_at"] == "2021-01-01T00:00:01+00:00"


def test_observed_trade_anchor_age_rule_conflict_and_legacy_policy() -> None:
    observed = build_lifecycle_catalog(
        _archive(),
        pd.DataFrame(),
        pd.DataFrame(),
        first_observed_trades=_trade("2021-01-01T00:00:00+00:00"),
        scope_registry_records=_scope(),
        announcement_search_completed=True,
    )
    assert observed.iloc[0]["eligibility_age_anchor_basis"] == (
        "first_observed_binance_futures_trade"
    )
    bars = pd.DataFrame(
        {
            "symbol": ["ABCUSDT", "ABCUSDT"],
            "close_time": pd.to_datetime(
                ["2021-01-30T23:59:59.999Z", "2021-01-31T00:00:00Z"],
                utc=True,
                format="mixed",
            ),
            "close": [1.0, 1.0],
        }
    )
    result = apply_point_in_time_eligibility(bars, observed)
    assert result["is_eligible"].tolist() == [False, True]

    conflict = build_lifecycle_catalog(
        _archive(),
        pd.DataFrame(),
        _listing("2021-01-02T00:00:00+00:00"),
        first_observed_trades=_trade("2021-01-01T00:00:00+00:00"),
        scope_registry_records=_scope(),
        announcement_search_completed=True,
    )
    assert conflict.iloc[0]["eligibility_age_anchor_basis"] == "unresolved"
    assert conflict.iloc[0]["age_anchor_conflict_status"] == (
        "first_observed_trade_precedes_claimed_exact_launch"
    )

    legacy = build_lifecycle_catalog(
        _archive(),
        pd.DataFrame(),
        pd.DataFrame(),
        first_observed_trades=_trade("2019-12-15T00:00:00+00:00"),
        scope_registry_records=_scope(),
        announcement_search_completed=True,
    )
    assert legacy.iloc[0]["eligibility_age_anchor_basis"] == (
        "first_observed_binance_futures_trade"
    )
    assert legacy.iloc[0]["age_anchor_conflict_status"] == "none"


def _trade_zip(rows: list[str]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("ABCUSDT-trades-2021-01-01.csv", "\n".join(rows))
    return output.getvalue()


def _index(keys: list[str]) -> bytes:
    contents = "".join(f"<Contents><Key>{key}</Key></Contents>" for key in keys)
    return (
        f'<ListBucketResult xmlns="{S3_NAMESPACE}"><KeyCount>{len(keys)}</KeyCount>'
        f"{contents}<IsTruncated>false</IsTruncated></ListBucketResult>"
    ).encode()


def test_daily_trade_probe_uses_only_earliest_archive_and_true_minimum(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    early = "data/futures/um/daily/trades/ABCUSDT/ABCUSDT-trades-2021-01-01.zip"
    later = "data/futures/um/daily/trades/ABCUSDT/ABCUSDT-trades-2021-01-02.zip"
    payload = _trade_zip(
        [
            "id,price,qty,quote_qty,time,is_buyer_maker",
            "2,1,1,1,1609459202000,true",
            "1,1,1,1,1609459201000,false",
        ]
    )
    digest = hashlib.sha256(payload).hexdigest()
    requested: list[str] = []

    def read(url: str, timeout: int = 60) -> bytes:
        requested.append(url)
        if "s3-ap" in url:
            return _index([early, f"{early}.CHECKSUM", later, f"{later}.CHECKSUM"])
        if url.endswith(".CHECKSUM"):
            return f"{digest}  {early.rsplit('/', 1)[-1]}".encode()
        if early in url:
            return payload
        raise AssertionError("The later archive must not be downloaded")

    monkeypatch.setattr(public_data, "_read_url", read)
    evidence = acquire_first_observed_trade("ABCUSDT", tmp_path)
    assert evidence.archive_object_key == early
    assert evidence.earliest_trade_timestamp == "2021-01-01T00:00:01+00:00"
    assert not any(later in url and "s3-ap" not in url for url in requested)


@pytest.mark.parametrize(
    "rows,match",
    [
        (["id,price,qty,quote_qty,time", "1,1,1,1,1.5"], "exact integer"),
        (["id,price,qty,quote_qty,time"], "no trades"),
    ],
)
def test_trade_parser_rejects_malformed_or_empty(
    tmp_path: Path, rows: list[str], match: str
) -> None:
    path = tmp_path / "trades.zip"
    path.write_bytes(_trade_zip(rows))
    with pytest.raises(ValueError, match=match):
        parse_earliest_trade_timestamp(path)


def test_trade_key_grammar_and_unicode_identity_are_strict() -> None:
    key = "data/futures/um/daily/trades/龙虾USDT/龙虾USDT-trades-2026-03-11.zip"
    assert validate_daily_trade_object_key(key).symbol == "龙虾USDT"
    with pytest.raises(ValueError):
        validate_daily_trade_object_key(
            "data/futures/um/daily/trades/../ABCUSDT/ABCUSDT-trades-2021-01-01.zip"
        )


def _announcement(lines: list[str], event_type: str = "listing"):
    action = "Launch" if event_type == "listing" else "Settle"
    return parse_announcement_evidence(
        {
            "code": "000000",
            "data": {
                "title": f"Binance Futures Will {action} ABCUSDT Perpetual Contract",
                "body": json.dumps({"child": [{"text": line} for line in lines]}),
            },
        },
        {"code": "article", "releaseDate": 1_735_556_100_000},
        event_type,
        {"ABCUSDT"},
        retrieved_at=None,
        raw_snapshot_path="/raw.json",
        raw_snapshot_sha256="a" * 64,
    )[0]


def test_announcement_context_terminates_across_mixed_purpose_rows() -> None:
    copy_row = _announcement(
        [
            "Binance Futures will launch:",
            "Copy Trading adds ABCUSDT at 2024-12-30 11:30 (UTC).",
        ]
    )
    assert copy_row.official_event_at is None
    operational = _announcement(
        [
            "Binance Futures will launch:",
            "Maintenance ends at 2024-12-30 10:30 (UTC).",
            "2024-12-30 11:30 (UTC): ABCUSDT",
        ]
    )
    assert operational.official_event_at is None
    genuine = _announcement(
        ["Binance Futures will launch ABCUSDT at 2024-12-30 11:30 (UTC)."]
    )
    assert genuine.official_event_at == "2024-12-30T11:30:00+00:00"


def test_legacy_launch_table_maps_exact_symbol_and_time() -> None:
    row = _announcement(
        [
            "USDⓈ- M Perpetual Contracts",
            "ABCUSDT",
            "Launch Time",
            "2024-12-30 11:30 (UTC)",
            "Multi-Asset Mode Supported",
        ]
    )
    assert row.official_event_at == "2024-12-30T11:30:00+00:00"


@pytest.mark.parametrize(
    "title",
    [
        "Binance Futures Will Settle ABCUSDT Perpetual Contract",
        "Binance Futures Will Cease Trading ABCUSDT Perpetual Contract",
        "Binance Futures Will Close All Positions for ABCUSDT Perpetual Contract",
    ],
)
def test_delisting_positive_semantics_do_not_require_literal_delist(title: str) -> None:
    assert classify_article_semantics(title, "") == "delisting_or_settlement"


def _exchange_item(symbol: str, *, subtype: list[str] | None = None) -> dict:
    return {
        "symbol": symbol,
        "baseAsset": symbol.removesuffix("USDT"),
        "quoteAsset": "USDT",
        "marginAsset": "USDT",
        "contractType": "PERPETUAL",
        "underlyingType": "COIN",
        "underlyingSubType": subtype or ["Layer-1"],
    }


def test_scope_registry_is_candidate_bound_and_uses_reviewed_negatives() -> None:
    candidates = ["USTCUSDT", "FRAXUSDT", "JUPUSDT", "SYRUPUSDT", "LEVUSDT", "龙虾USDT"]
    payload = {
        "symbols": [
            _exchange_item("USTCUSDT"),
            _exchange_item("FRAXUSDT"),
            _exchange_item("JUPUSDT"),
            _exchange_item("SYRUPUSDT"),
            _exchange_item("LEVUSDT", subtype=["Leveraged Token"]),
            _exchange_item("龙虾USDT", subtype=["Chinese"]),
        ]
    }
    registry = build_reviewed_scope_registry_fixture(
        candidates,
        payload,
        audited_at="2026-01-01T00:00:00+00:00",
    )
    records = verify_scope_registry(registry, candidates, require_independent_review=False)
    assert registry["candidate_set_digest"] == candidate_set_digest(candidates)
    assert records["USTCUSDT"]["product_scope"] == "excluded_stablecoin"
    assert records["FRAXUSDT"]["product_scope"] == "excluded_stablecoin"
    assert records["LEVUSDT"]["product_scope"] == "excluded_leveraged_token"
    assert records["JUPUSDT"]["leveraged_evidence_status"] == (
        "reviewed_finite_universe_negative"
    )
    assert records["SYRUPUSDT"]["product_scope"] == "in_scope_crypto_perpetual"
    assert records["龙虾USDT"]["safe_storage_component"].isascii()
    with pytest.raises(ValueError, match="candidate identities changed"):
        verify_scope_registry(
            registry, [*candidates, "NEWUSDT"], require_independent_review=False
        )


def _checkpoint_fixture(tmp_path: Path) -> tuple[Path, Path]:
    raw = tmp_path / "raw-run"
    index = raw / "archive_index"
    index.mkdir(parents=True)
    symbol_xml = index / "symbols.xml"
    symbol_xml.write_bytes(
        (
            f'<ListBucketResult xmlns="{S3_NAMESPACE}"><CommonPrefixes><Prefix>'
            "data/futures/um/monthly/klines/ABCUSDT/"
            "</Prefix></CommonPrefixes><IsTruncated>false</IsTruncated></ListBucketResult>"
        ).encode()
    )
    month_xml = index / "abc.xml"
    key = "data/futures/um/monthly/klines/ABCUSDT/1h/ABCUSDT-1h-2020-01.zip"
    month_xml.write_bytes(_index([key, f"{key}.CHECKSUM"]))
    record_new_snapshot_provenance(symbol_xml, url="https://official/symbols", parser_version="v1")
    record_new_snapshot_provenance(month_xml, url="https://official/abc", parser_version="v1")
    checkpoint = {
        "schema_version": ARCHIVE_CHECKPOINT_SCHEMA_VERSION,
        "canonical_symbols": ["ABCUSDT"],
        "quarantined_prefixes": [],
        "symbol_audit": {"unique_prefix_count": 1},
        "symbol_raw_snapshot_paths": [str(symbol_xml.resolve())],
        "symbol_raw_snapshot_sha256s": [sha256_path(symbol_xml)],
        "archive_observations": [
            {
                "symbol": "ABCUSDT",
                "first_archive_month": "2020-01",
                "last_archive_month": "2020-01",
                "unique_archive_count": 1,
                "observed_archive_object_keys": [key],
                "archive_raw_snapshot_paths": [str(month_xml.resolve())],
                "archive_raw_snapshot_sha256s": [sha256_path(month_xml)],
            }
        ],
    }
    path = raw / "archive_checkpoint.json"
    path.write_text(json.dumps(checkpoint))
    return path, raw


def test_checkpoint_reconstructs_raw_xml_and_rejects_mutation(tmp_path: Path) -> None:
    checkpoint, raw = _checkpoint_fixture(tmp_path)
    state = verify_archive_checkpoint(checkpoint, raw)
    assert state["archive_observations"][0]["first_archive_month"] == "2020-01"
    payload = json.loads(checkpoint.read_text())
    Path(payload["archive_observations"][0]["archive_raw_snapshot_paths"][0]).write_bytes(
        b"mutated"
    )
    with pytest.raises(ValueError, match="hash mismatch"):
        verify_archive_checkpoint(checkpoint, raw)


def test_checkpoint_rejects_path_escape_and_derived_disagreement(tmp_path: Path) -> None:
    checkpoint, raw = _checkpoint_fixture(tmp_path)
    payload = json.loads(checkpoint.read_text())
    outside = tmp_path / "outside.xml"
    outside.write_bytes(b"outside")
    payload["archive_observations"][0]["archive_raw_snapshot_paths"] = [
        str(outside.resolve())
    ]
    payload["archive_observations"][0]["archive_raw_snapshot_sha256s"] = [
        sha256_path(outside)
    ]
    checkpoint.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="escapes"):
        verify_archive_checkpoint(checkpoint, raw)

    checkpoint, raw = _checkpoint_fixture(tmp_path / "second")
    payload = json.loads(checkpoint.read_text())
    payload["archive_observations"][0]["first_archive_month"] = "2020-02"
    checkpoint.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="derived field"):
        verify_archive_checkpoint(checkpoint, raw)


def test_approval_pin_rejects_missing_stale_and_changed_review(tmp_path: Path) -> None:
    review = tmp_path / "review.md"
    review.write_text("PASS")
    bundle = {
        "bundle_id": "a" * 64,
        "code_commit": "b" * 40,
        "config": {"sha256": "c" * 64},
    }
    verified = {"bundle": bundle}
    state_path = tmp_path / "approval-state.json"
    core = {
        "schema_version": APPROVAL_SCHEMA_VERSION,
        "lifecycle_bundle_id": bundle["bundle_id"],
        "lifecycle_evidence_code_commit": bundle["code_commit"],
        "config_sha256": bundle["config"]["sha256"],
        "independent_review_artifact": {
            "identifier": "review-1",
            "path": str(review.resolve()),
            "sha256": sha256_path(review),
        },
        "approval_purpose": "full_history_acquisition_after_lifecycle_audit",
        "approval_status": "active",
        "approval_state_registry": {"path": str(state_path.resolve())},
        "approved_at": "2026-01-01T00:00:00+00:00",
    }
    approval = {**core, "approval_id": content_identity(core)}
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
    state_path.write_text(
        json.dumps({**state_core, "registry_id": content_identity(state_core)})
    )
    path = tmp_path / "approval.json"
    path.write_text(json.dumps(approval))
    assert verify_approval_pin(path, verified)["approval"]["approval_status"] == "active"
    with pytest.raises(FileNotFoundError):
        verify_approval_pin(tmp_path / "no-real-approval.json", verified)
    review.write_text("changed")
    with pytest.raises(ValueError, match="review artifact digest"):
        verify_approval_pin(path, verified)
