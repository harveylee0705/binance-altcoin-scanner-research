from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

import alt_hot_scanner.data.binance_public as public_data
from alt_hot_scanner.data.announcements import parse_announcement_evidence
from alt_hot_scanner.data.binance_public import discover_archive_months
from alt_hot_scanner.data.normalize import normalize_kline_frame
from alt_hot_scanner.data.provenance import (
    load_snapshot_provenance,
    record_new_snapshot_provenance,
)
from alt_hot_scanner.universe.authorization import (
    APPROVAL_SCHEMA_VERSION,
    APPROVAL_STATE_SCHEMA_VERSION,
    PLAN_SCHEMA_VERSION,
    build_bundle_payload,
    build_plan_integrity,
    catalog_readiness,
    content_identity,
    sha256_path,
    verify_bound_plan,
    verify_lifecycle_bundle,
)
from alt_hot_scanner.universe.contracts import records_from_exchange_info
from alt_hot_scanner.universe.lifecycle import (
    build_lifecycle_catalog,
    catalog_coverage,
)
from alt_hot_scanner.universe.scope_registry import (
    SCOPE_REVIEW_SCHEMA_VERSION,
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


def test_completed_delisting_search_without_timestamp_does_not_remove_history() -> None:
    archives = pd.DataFrame([_archive("AAAUSDT"), _archive("BBBUSDT")])
    current = pd.DataFrame([_current("AAAUSDT", "TRADING"), _current("BBBUSDT", "SETTLING")])
    evidence = pd.DataFrame([_listing("AAAUSDT"), _listing("BBBUSDT")])
    catalog = build_lifecycle_catalog(
        archives, current, evidence, announcement_search_completed=True
    ).set_index("symbol")
    assert catalog.loc["AAAUSDT", "historical_inclusion_readiness"] == "ready"
    assert catalog.loc["AAAUSDT", "delisting_evidence_state"] == "not_applicable_currently_trading"
    assert catalog.loc["BBBUSDT", "historical_inclusion_readiness"] == "ready"
    assert catalog.loc["BBBUSDT", "delisting_evidence_state"] == (
        "official_search_completed_no_reliable_announcement_timestamp"
    )


def _write_bundle(tmp_path: Path) -> tuple[Path, pd.DataFrame]:
    (tmp_path / "config").mkdir()
    review_dir = tmp_path / "docs" / "reviews"
    review_dir.mkdir(parents=True)
    catalog = build_lifecycle_catalog(
        pd.DataFrame([_archive("AAAUSDT")]),
        pd.DataFrame([_current("AAAUSDT", "TRADING")]),
        pd.DataFrame([_listing("AAAUSDT")]),
        announcement_search_completed=True,
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
    (tmp_path / "announcement_evidence.json").write_text(json.dumps([_listing("AAAUSDT")]))
    (tmp_path / "archive_observations.json").write_text(json.dumps([_archive("AAAUSDT")]))
    queue = {"status": "reviewed_finite_universe", "prefixes": [], "dispositions": {}}
    (tmp_path / "noncanonical_archive_prefix_queue.json").write_text(json.dumps(queue))
    readiness = catalog_readiness(catalog, [])
    (tmp_path / "readiness.json").write_text(json.dumps(readiness))
    coverage = catalog_coverage(catalog)
    coverage["recomputed_readiness"] = readiness
    (tmp_path / "coverage.json").write_text(json.dumps(coverage))
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
            }
        ]
    }
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
    (tmp_path / "first_observed_trades.json").write_text("[]")
    (tmp_path / "announcement_corpus_audit.json").write_text(
        json.dumps(
            {
                "catalogs": [
                    {
                        "event_type": "delisting",
                        "inspection_policy": "complete_catalog_detail_inspection",
                        "candidate_articles": 0,
                        "declared_total": 0,
                    }
                ]
            }
        )
    )
    inventory = {
        "candidate_set_digest": registry["candidate_set_digest"],
        "candidate_identities": ["AAAUSDT"],
    }
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
    (tmp_path / "lifecycle_daily_trade_boundaries.json").write_text("[]")
    primitive_core = {"schema_version": "primitive-evidence-manifest-v1", "entries": []}
    (tmp_path / "primitive_evidence_manifest.json").write_text(
        json.dumps({**primitive_core, "manifest_id": content_identity(primitive_core)})
    )
    replay_core = {
        "schema_version": "lifecycle-full-evidence-replay-v1",
        "status": "PASS",
    }
    (tmp_path / "full_evidence_verification_report.json").write_text(
        json.dumps(
            {
                **replay_core,
                "verification_report_id": content_identity(replay_core),
            }
        )
    )
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
        "full_evidence_verification_report.json",
    ]
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
