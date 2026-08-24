from __future__ import annotations

import json
from datetime import UTC, datetime
from itertools import pairwise
from typing import Any

import numpy as np
import pandas as pd

from alt_hot_scanner.identity import require_archive_symbol_identity

CATALOG_SCHEMA_VERSION = "binance-usdm-lifecycle-v5"
PARSER_VERSION = "lifecycle-catalog-v5"

CATALOG_COLUMNS = [
    "catalog_schema_version",
    "contract_identity",
    "symbol",
    "base_asset",
    "quote_asset",
    "margin_asset",
    "market_family",
    "product_family",
    "contract_type",
    "underlying_type",
    "underlying_subtype",
    "is_crypto_underlying",
    "is_stablecoin_underlying",
    "is_leveraged_token",
    "stablecoin_evidence_status",
    "stablecoin_conflict_status",
    "leveraged_evidence_status",
    "leveraged_conflict_status",
    "is_benchmark_btc",
    "is_eth",
    "scope_classification_status",
    "scope_classification_provenance",
    "scope_disposition",
    "listing_announcement_published_at",
    "exact_official_trading_start_at",
    "official_trading_start_at",
    "first_observed_trade_at",
    "first_observed_trade_archive_key",
    "first_observed_trade_published_sha256",
    "first_observed_trade_computed_sha256",
    "first_observed_trade_raw_path",
    "first_observed_trade_retrieved_at",
    "first_observed_trade_parser_version",
    "first_observed_trade_evidence_status",
    "eligibility_age_anchor_at",
    "eligibility_age_anchor_basis",
    "age_anchor_conflict_status",
    "conservative_anchor_coverage_loss_start_at",
    "exchange_info_onboard_at",
    "first_archive_month",
    "first_valid_kline_at",
    "delisting_announcement_published_at",
    "official_last_trading_at",
    "last_archive_month",
    "last_valid_kline_at",
    "exchange_info_delivery_at",
    "latest_known_status",
    "present_in_current_exchange_info",
    "scope_classification_complete",
    "listing_start_complete",
    "delisting_evidence_state",
    "current_status_warning",
    "onboard_start_discrepancy_seconds",
    "onboard_start_discrepancy_status",
    "historical_inclusion_readiness",
    "lifecycle_episode_count",
    "lifecycle_intervals",
    "lifecycle_adjudication_status",
    "lifecycle_adjudication_id",
    "listing_evidence_status",
    "listing_source_type",
    "listing_source_url",
    "listing_article_id",
    "listing_retrieved_at",
    "listing_raw_snapshot_path",
    "listing_raw_snapshot_sha256",
    "listing_parser_version",
    "delisting_evidence_status",
    "delisting_source_type",
    "delisting_source_url",
    "delisting_article_id",
    "delisting_retrieved_at",
    "delisting_raw_snapshot_path",
    "delisting_raw_snapshot_sha256",
    "delisting_parser_version",
    "archive_discovery_timestamp",
    "archive_source_url",
    "archive_discovery_provenance",
    "archive_parser_version",
    "archive_raw_snapshot_paths",
    "archive_raw_snapshot_sha256s",
    "metadata_acquired_at",
    "metadata_source",
    "metadata_raw_snapshot_path",
    "metadata_raw_snapshot_sha256",
    "catalog_created_at",
]


def _null_record(symbol: str, created_at: pd.Timestamp) -> dict[str, Any]:
    return {
        **{column: None for column in CATALOG_COLUMNS},
        "catalog_schema_version": CATALOG_SCHEMA_VERSION,
        "contract_identity": symbol,
        "symbol": symbol,
        "quote_asset": "USDT" if symbol.endswith("USDT") else None,
        "market_family": "USDM",
        "product_family": "FUTURES",
        "is_benchmark_btc": symbol == "BTCUSDT",
        "is_eth": symbol == "ETHUSDT",
        "scope_classification_status": "unresolved",
        "scope_disposition": "unresolved",
        "listing_evidence_status": "unresolved",
        "delisting_evidence_status": "unresolved",
        "stablecoin_evidence_status": "unresolved",
        "stablecoin_conflict_status": "none",
        "leveraged_evidence_status": "unresolved",
        "leveraged_conflict_status": "none",
        "scope_classification_complete": False,
        "listing_start_complete": False,
        "eligibility_age_anchor_basis": "unresolved",
        "age_anchor_conflict_status": "none",
        "delisting_evidence_state": "not_investigated",
        "onboard_start_discrepancy_status": "not_comparable",
        "historical_inclusion_readiness": "blocked",
        "lifecycle_episode_count": 0,
        "lifecycle_intervals": [],
        "lifecycle_adjudication_status": "not_required",
        "present_in_current_exchange_info": False,
        "catalog_created_at": created_at,
    }


def _json_sequence(value: object) -> str | None:
    if value is None or (not isinstance(value, (list, tuple)) and pd.isna(value)):
        return None
    return json.dumps(list(value), ensure_ascii=True, separators=(",", ":"))


def _accepted_event(evidence: pd.DataFrame, symbol: str, event_type: str) -> pd.Series | None:
    if evidence.empty:
        return None
    matched = evidence.loc[
        evidence["symbol"].eq(symbol)
        & evidence["event_type"].eq(event_type)
        & evidence["match_status"].eq("accepted")
    ]
    if len(matched) != 1:
        return None
    return matched.iloc[0]


def _article_event(
    evidence: pd.DataFrame, symbol: str, event_type: str, article_id: str
) -> pd.Series:
    matched = evidence.loc[
        evidence["symbol"].eq(symbol)
        & evidence["event_type"].eq(event_type)
        & evidence["article_code"].eq(article_id)
    ]
    if len(matched) != 1:
        raise ValueError(f"Adjudicated {event_type} article is not unique for {symbol}")
    expected = "original_perpetual_launch" if event_type == "listing" else (
        "delisting_or_settlement"
    )
    if matched.iloc[0]["article_semantic_class"] != expected:
        raise ValueError(f"Adjudicated {event_type} article has the wrong product semantics")
    return matched.iloc[0]


def _iso(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).isoformat()


def _adjudicated_intervals(
    record: dict[str, Any],
    evidence: pd.DataFrame,
    adjudication: dict[str, Any],
    adjudication_id: str,
    episode_trade_by_id: dict[str, dict[str, Any]],
    cutoff_by_id: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    symbol = record["symbol"]
    intervals: list[dict[str, Any]] = []
    rejected = adjudication.get("rejected_listing_articles", [])
    for rejected_item in rejected:
        _article_event(evidence, symbol, "listing", rejected_item["article_id"])
    for spec in adjudication["episodes"]:
        basis = "first_observed_binance_futures_trade"
        listing = None
        if spec.get("listing_article_id") is not None:
            listing = _article_event(evidence, symbol, "listing", spec["listing_article_id"])
        trade = episode_trade_by_id.get(spec["episode_id"])
        anchor = None if trade is None else _iso(trade.get("earliest_trade_timestamp"))
        if anchor is None:
            raise ValueError(f"Adjudicated lifecycle episode lacks a live anchor for {symbol}")
        reviewed_cutoff = cutoff_by_id.get(spec["episode_id"])
        if reviewed_cutoff is None:
            raise ValueError("Adjudicated lifecycle episode lacks reviewed delisting disposition")
        cutoff = _iso(reviewed_cutoff.get("official_publication_timestamp"))
        terminated = _iso(spec.get("terminated_at"))
        if terminated is None:
            terminated = _iso(reviewed_cutoff.get("terminal_last_trading_at"))
        interval = {
            "symbol": symbol,
            "lifecycle_episode_id": spec["episode_id"],
            "age_live_anchor_at": anchor,
            "anchor_basis": basis,
            "eligible_from": (pd.Timestamp(anchor) + pd.Timedelta(days=30)).isoformat(),
            "listing_article_id": None if listing is None else listing["article_code"],
            "listing_source_url": None if listing is None else listing["source_url"],
            "listing_raw_snapshot_sha256": (
                None if listing is None else listing["raw_snapshot_sha256"]
            ),
            "delisting_announcement_published_at": cutoff,
            "delisting_article_id": reviewed_cutoff.get("article_code"),
            "delisting_source_url": reviewed_cutoff.get("official_article_url"),
            "delisting_raw_snapshot_sha256": reviewed_cutoff.get("raw_article_sha256"),
            "last_trading_at": terminated,
            "termination_basis": spec.get("termination_basis"),
            "eligibility_end_at": cutoff or terminated,
            "interval_evidence_status": "reviewed_resolved",
            "conflicts": [item["reason"] for item in rejected],
        }
        intervals.append(interval)

    for previous, current in pairwise(intervals):
        if previous["last_trading_at"] is None:
            raise ValueError(f"Non-final lifecycle episode lacks termination evidence for {symbol}")
        if pd.Timestamp(previous["last_trading_at"]) >= pd.Timestamp(
            current["age_live_anchor_at"]
        ):
            raise ValueError(f"Lifecycle episodes overlap for {symbol}")
    record["lifecycle_adjudication_status"] = "reviewed_resolved"
    record["lifecycle_adjudication_id"] = adjudication_id
    return intervals


def build_lifecycle_catalog(
    archive_observations: pd.DataFrame,
    exchange_info_records: pd.DataFrame,
    announcement_evidence: pd.DataFrame | None = None,
    *,
    created_at: pd.Timestamp | None = None,
    announcement_search_completed: bool | None = None,
    first_observed_trades: pd.DataFrame | None = None,
    episode_first_observed_trades: list[dict[str, Any]] | None = None,
    delisting_registry_records: list[dict[str, Any]] | None = None,
    scope_registry_records: list[dict[str, Any]] | None = None,
    lifecycle_adjudications: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Merge distinct evidence tiers without promoting observed bounds into exact events."""
    required_archive = {
        "symbol",
        "first_archive_month",
        "last_archive_month",
        "archive_discovery_timestamp",
        "archive_source_url",
        "archive_discovery_provenance",
    }
    missing = required_archive - set(archive_observations.columns)
    if missing:
        raise ValueError(f"Archive observations are missing {sorted(missing)}")
    if archive_observations["symbol"].duplicated().any():
        raise ValueError("Archive observations must have one row per symbol")
    if not exchange_info_records.empty and exchange_info_records["symbol"].duplicated().any():
        raise ValueError("Current exchangeInfo must have one row per symbol")

    evidence = announcement_evidence if announcement_evidence is not None else pd.DataFrame()
    search_completed = (
        announcement_evidence is not None
        if announcement_search_completed is None
        else announcement_search_completed
    )
    created = created_at or pd.Timestamp(datetime.now(UTC))
    current_by_symbol = (
        exchange_info_records.set_index("symbol", drop=False)
        if not exchange_info_records.empty
        else pd.DataFrame()
    )
    trades = first_observed_trades if first_observed_trades is not None else pd.DataFrame()
    if not trades.empty and trades["symbol"].duplicated().any():
        raise ValueError("First-observed-trade evidence must have one row per symbol")
    trades_by_symbol = trades.set_index("symbol", drop=False) if not trades.empty else pd.DataFrame()
    episode_trade_by_id = {
        row["lifecycle_episode_id"]: row for row in (episode_first_observed_trades or [])
    }
    if len(episode_trade_by_id) != len(episode_first_observed_trades or []):
        raise ValueError("Episode first-trade evidence contains duplicate episodes")
    cutoff_by_id = {
        row["lifecycle_episode_id"]: row for row in (delisting_registry_records or [])
    }
    if len(cutoff_by_id) != len(delisting_registry_records or []):
        raise ValueError("Delisting registry contains duplicate episodes")
    scope_by_symbol = {
        row.get("contract_identity"): row for row in (scope_registry_records or [])
    }
    if len(scope_by_symbol) != len(scope_registry_records or []):
        raise ValueError("Scope registry records contain duplicate identities")
    adjudication_by_symbol = (
        lifecycle_adjudications.get("by_symbol", {}) if lifecycle_adjudications else {}
    )
    adjudication_id = (
        lifecycle_adjudications.get("adjudication_id") if lifecycle_adjudications else None
    )
    records: list[dict[str, Any]] = []
    for archive_row in archive_observations.sort_values("symbol").to_dict("records"):
        symbol = require_archive_symbol_identity(archive_row["symbol"], "archive symbol")
        record = _null_record(symbol, created)
        record.update(
            {
                "first_archive_month": archive_row["first_archive_month"],
                "last_archive_month": archive_row["last_archive_month"],
                "archive_discovery_timestamp": archive_row["archive_discovery_timestamp"],
                "archive_source_url": archive_row["archive_source_url"],
                "archive_discovery_provenance": archive_row["archive_discovery_provenance"],
                "archive_parser_version": archive_row.get("archive_parser_version"),
                "archive_raw_snapshot_paths": _json_sequence(
                    archive_row.get("archive_raw_snapshot_paths")
                ),
                "archive_raw_snapshot_sha256s": _json_sequence(
                    archive_row.get("archive_raw_snapshot_sha256s")
                ),
            }
        )

        if not current_by_symbol.empty and symbol in current_by_symbol.index:
            current = current_by_symbol.loc[symbol]
            record["present_in_current_exchange_info"] = True
            for column in [
                "base_asset",
                "quote_asset",
                "margin_asset",
                "market_family",
                "product_family",
                "contract_type",
                "underlying_type",
                "is_crypto_underlying",
                "is_stablecoin_underlying",
                "is_leveraged_token",
                "stablecoin_evidence_status",
                "stablecoin_conflict_status",
                "leveraged_evidence_status",
                "leveraged_conflict_status",
                "is_benchmark_btc",
                "is_eth",
                "scope_classification_status",
                "exchange_info_onboard_at",
                "exchange_info_delivery_at",
                "latest_known_status",
                "metadata_acquired_at",
                "metadata_source",
                "metadata_raw_snapshot_path",
                "metadata_raw_snapshot_sha256",
            ]:
                record[column] = current.get(column)
            record["underlying_subtype"] = _json_sequence(current.get("underlying_subtype"))
            record["scope_classification_provenance"] = current.get(
                "scope_classification_provenance"
            )
            if not scope_registry_records and record["scope_classification_status"] != "unresolved":
                record["scope_disposition"] = (
                    "benchmark_only"
                    if symbol == "BTCUSDT"
                    else "in_scope_crypto_perpetual"
                    if bool(record["is_crypto_underlying"])
                    and not bool(record["is_stablecoin_underlying"])
                    and not bool(record["is_leveraged_token"])
                    else "excluded"
                )

        scope = scope_by_symbol.get(symbol)
        if scope is not None:
            record.update(
                {
                    "base_asset": scope.get("base_asset") or record.get("base_asset"),
                    "quote_asset": scope.get("quote_asset") or record.get("quote_asset"),
                    "is_crypto_underlying": scope.get("is_crypto_underlying"),
                    "is_stablecoin_underlying": scope.get("is_stablecoin_underlying"),
                    "is_leveraged_token": scope.get("is_leveraged_token"),
                    "stablecoin_evidence_status": scope.get("stablecoin_evidence_status"),
                    "leveraged_evidence_status": scope.get("leveraged_evidence_status"),
                    "scope_classification_status": (
                        "resolved_reviewed_finite_universe"
                        if scope.get("scope_audit_status") == "complete"
                        else "unresolved"
                    ),
                    "scope_classification_provenance": (
                        "candidate_set_bound_historical_scope_registry"
                    ),
                    "scope_disposition": scope.get("product_scope"),
                }
            )

        listing = _accepted_event(evidence, symbol, "listing")
        if listing is not None:
            record.update(
                {
                    "listing_announcement_published_at": listing["article_published_at"],
                    "exact_official_trading_start_at": listing["official_event_at"],
                    "official_trading_start_at": listing["official_event_at"],
                    "listing_evidence_status": listing["event_time_evidence_status"],
                    "listing_source_type": "official_binance_structured_announcement",
                    "listing_source_url": listing["source_url"],
                    "listing_article_id": listing["article_code"],
                    "listing_retrieved_at": listing["retrieved_at"],
                    "listing_raw_snapshot_path": listing["raw_snapshot_path"],
                    "listing_raw_snapshot_sha256": listing["raw_snapshot_sha256"],
                    "listing_parser_version": listing["parser_version"],
                }
            )

        if not trades_by_symbol.empty and symbol in trades_by_symbol.index:
            trade = trades_by_symbol.loc[symbol]
            record.update(
                {
                    "first_observed_trade_at": trade["earliest_trade_timestamp"],
                    "first_observed_trade_archive_key": trade["archive_object_key"],
                    "first_observed_trade_published_sha256": trade["published_sha256"],
                    "first_observed_trade_computed_sha256": trade["computed_sha256"],
                    "first_observed_trade_raw_path": trade["raw_path"],
                    "first_observed_trade_retrieved_at": trade["original_retrieval_timestamp"],
                    "first_observed_trade_parser_version": trade["parser_version"],
                    "first_observed_trade_evidence_status": trade["evidence_status"],
                }
            )
        scope_is_excluded = str(record["scope_disposition"]).startswith("excluded")
        classification_complete = record["scope_classification_status"] != "unresolved" and (
            scope_is_excluded
            or (
                isinstance(record["is_stablecoin_underlying"], (bool, np.bool_))
                and isinstance(record["is_leveraged_token"], (bool, np.bool_))
                and record["stablecoin_conflict_status"] != "conflict"
                and record["leveraged_conflict_status"] != "conflict"
            )
        )
        exact_start = record["exact_official_trading_start_at"]
        observed_start = record["first_observed_trade_at"]
        anchor = None
        anchor_basis = "unresolved"
        anchor_conflict = "none"
        if exact_start is not None and observed_start is not None and pd.Timestamp(
            observed_start
        ) < pd.Timestamp(exact_start):
            anchor_conflict = "first_observed_trade_precedes_claimed_exact_launch"
        if observed_start is not None:
            anchor = observed_start
            anchor_basis = "first_observed_binance_futures_trade"
        is_current = record["present_in_current_exchange_info"] is True
        status = record["latest_known_status"]
        is_trading = is_current and status == "TRADING"
        exact_delisting = record["delisting_announcement_published_at"] is not None
        matching_delisting_rows = (
            evidence.loc[
                evidence["symbol"].eq(symbol) & evidence["event_type"].eq("delisting")
            ]
            if not evidence.empty
            else pd.DataFrame()
        )
        has_delisting_conflict = (
            not matching_delisting_rows.empty
            and matching_delisting_rows["match_status"].astype(str).str.contains("ambiguous").any()
        ) or (exact_delisting and is_trading)
        adjudication = adjudication_by_symbol.get(symbol)
        in_research_scope = record["scope_disposition"] in {
            "in_scope_crypto_perpetual",
            "benchmark_only",
        }
        if adjudication is not None:
            intervals = _adjudicated_intervals(
                record,
                evidence,
                adjudication,
                str(adjudication_id),
                episode_trade_by_id,
                cutoff_by_id,
            )
            first_interval = intervals[0]
            last_interval = intervals[-1]
            anchor = first_interval["age_live_anchor_at"]
            anchor_basis = first_interval["anchor_basis"]
            anchor_conflict = "none"
            first_listing_id = first_interval["listing_article_id"]
            if first_listing_id is not None:
                first_listing = _article_event(evidence, symbol, "listing", first_listing_id)
                record.update(
                    {
                        "listing_announcement_published_at": first_listing[
                            "article_published_at"
                        ],
                        "exact_official_trading_start_at": first_listing[
                            "official_event_at"
                        ],
                        "official_trading_start_at": first_listing["official_event_at"],
                        "listing_evidence_status": "reviewed_exact_episode_launch",
                        "listing_source_type": "official_binance_structured_announcement",
                        "listing_source_url": first_listing["source_url"],
                        "listing_article_id": first_listing["article_code"],
                        "listing_retrieved_at": first_listing["retrieved_at"],
                        "listing_raw_snapshot_path": first_listing["raw_snapshot_path"],
                        "listing_raw_snapshot_sha256": first_listing["raw_snapshot_sha256"],
                        "listing_parser_version": first_listing["parser_version"],
                    }
                )
            elif adjudication.get("rejected_listing_articles"):
                rejected_id = adjudication["rejected_listing_articles"][0]["article_id"]
                rejected_listing = _article_event(evidence, symbol, "listing", rejected_id)
                record.update(
                    {
                        "listing_announcement_published_at": rejected_listing[
                            "article_published_at"
                        ],
                        "exact_official_trading_start_at": None,
                        "official_trading_start_at": None,
                        "listing_evidence_status": "reviewed_scheduled_time_demoted",
                        "listing_source_type": "official_binance_structured_announcement",
                        "listing_source_url": rejected_listing["source_url"],
                        "listing_article_id": rejected_listing["article_code"],
                        "listing_retrieved_at": rejected_listing["retrieved_at"],
                        "listing_raw_snapshot_path": rejected_listing["raw_snapshot_path"],
                        "listing_raw_snapshot_sha256": rejected_listing[
                            "raw_snapshot_sha256"
                        ],
                        "listing_parser_version": rejected_listing["parser_version"],
                    }
                )
            if last_interval["delisting_article_id"] is not None:
                record.update(
                    {
                        "delisting_announcement_published_at": last_interval[
                            "delisting_announcement_published_at"
                        ],
                        "official_last_trading_at": last_interval["last_trading_at"],
                        "delisting_evidence_status": "reviewed_exact_episode_termination",
                        "delisting_source_type": "official_binance_structured_announcement",
                        "delisting_source_url": last_interval["delisting_source_url"],
                        "delisting_article_id": last_interval["delisting_article_id"],
                        "delisting_raw_snapshot_sha256": last_interval[
                            "delisting_raw_snapshot_sha256"
                        ],
                        "delisting_parser_version": "reviewed-delisting-registry-v1",
                    }
                )
            else:
                record["delisting_announcement_published_at"] = None
                record["official_last_trading_at"] = last_interval["last_trading_at"]
                record["delisting_article_id"] = None
            listing_complete = all(
                interval["interval_evidence_status"] == "reviewed_resolved"
                for interval in intervals
            )
            has_delisting_conflict = False
            delisting_state = (
                "resolved_multi_episode_currently_trading"
                if len(intervals) > 1 and last_interval["last_trading_at"] is None
                else "resolved_multi_episode_terminated"
                if len(intervals) > 1
                else "reviewed_terminal_episode"
                if last_interval["last_trading_at"] is not None
                else "not_applicable_currently_trading"
            )
            delisting_ready = True
        else:
            reviewed_cutoff = cutoff_by_id.get(f"{symbol}:1")
            if in_research_scope and reviewed_cutoff is None:
                has_delisting_conflict = True
            elif reviewed_cutoff is not None:
                record.update(
                    {
                        "delisting_announcement_published_at": reviewed_cutoff.get(
                            "official_publication_timestamp"
                        ),
                        "official_last_trading_at": reviewed_cutoff.get(
                            "terminal_last_trading_at"
                        ),
                        "delisting_evidence_status": reviewed_cutoff.get("review_status"),
                        "delisting_source_type": "reviewed_delisting_cutoff_registry",
                        "delisting_source_url": reviewed_cutoff.get("official_article_url"),
                        "delisting_article_id": reviewed_cutoff.get("article_code"),
                        "delisting_raw_snapshot_sha256": reviewed_cutoff.get(
                            "raw_article_sha256"
                        ),
                        "delisting_parser_version": "reviewed-delisting-registry-v1",
                    }
                )
                has_delisting_conflict = reviewed_cutoff.get("review_status") == (
                    "unresolved_conflicting_evidence"
                )
            exact_delisting = record["delisting_announcement_published_at"] is not None
            # Exact listing metadata is descriptive only. A verified trade anchor is
            # sufficient even when the announcement's scheduled time is contradicted.
            listing_complete = anchor is not None
            if has_delisting_conflict:
                delisting_state = "conflicting_evidence"
            elif exact_delisting:
                delisting_state = "exact_applicable_announcement_publication"
            elif is_trading:
                delisting_state = "not_applicable_currently_trading"
            elif search_completed:
                delisting_state = "official_search_completed_no_reliable_announcement_timestamp"
            else:
                delisting_state = "not_investigated"
            delisting_ready = not has_delisting_conflict and (
                is_trading
                or exact_delisting
                or delisting_state
                == "official_search_completed_no_reliable_announcement_timestamp"
            )
            intervals = []
            if in_research_scope and anchor is not None:
                intervals = [
                    {
                        "symbol": symbol,
                        "lifecycle_episode_id": f"{symbol}:1",
                        "age_live_anchor_at": _iso(anchor),
                        "anchor_basis": anchor_basis,
                        "eligible_from": (
                            pd.Timestamp(anchor) + pd.Timedelta(days=30)
                        ).isoformat(),
                        "listing_article_id": record.get("listing_article_id"),
                        "listing_source_url": record.get("listing_source_url"),
                        "listing_raw_snapshot_sha256": record.get(
                            "listing_raw_snapshot_sha256"
                        ),
                        "delisting_announcement_published_at": _iso(
                            record.get("delisting_announcement_published_at")
                        ),
                        "delisting_article_id": record.get("delisting_article_id"),
                        "delisting_source_url": record.get("delisting_source_url"),
                        "delisting_raw_snapshot_sha256": record.get(
                            "delisting_raw_snapshot_sha256"
                        ),
                        "last_trading_at": _iso(record.get("official_last_trading_at")),
                        "termination_basis": (
                            "official_delisting_event"
                            if record.get("official_last_trading_at") is not None
                            else None
                        ),
                        "eligibility_end_at": _iso(
                            record.get("delisting_announcement_published_at")
                        )
                        or _iso(record.get("official_last_trading_at")),
                        "interval_evidence_status": (
                            "reviewed_resolved"
                            if listing_complete and delisting_ready
                            else "blocked"
                        ),
                        "conflicts": [],
                    }
                ]
        onboard = record["exchange_info_onboard_at"]
        official = (
            intervals[-1]["age_live_anchor_at"]
            if is_current and len(intervals) > 1
            else record["official_trading_start_at"]
        )
        if onboard is not None and official is not None:
            difference = abs(
                (pd.Timestamp(official) - pd.Timestamp(onboard)).total_seconds()
            )
            discrepancy_status = (
                "explained_by_reviewed_relisting"
                if len(intervals) > 1 and difference <= 3600
                else
                "consistent_within_engineering_threshold"
                if difference <= 3600
                else "unresolved_material_discrepancy"
            )
        else:
            difference = None
            discrepancy_status = "not_comparable"
        ready = (
            classification_complete
            and in_research_scope
            and listing_complete
            and delisting_ready
            and bool(intervals)
            and all(
                interval["interval_evidence_status"]
                in {"resolved", "reviewed_resolved"}
                for interval in intervals
            )
        )
        record.update(
            {
                "scope_classification_complete": classification_complete,
                "listing_start_complete": listing_complete,
                "eligibility_age_anchor_at": anchor,
                "eligibility_age_anchor_basis": anchor_basis,
                "age_anchor_conflict_status": anchor_conflict,
                "conservative_anchor_coverage_loss_start_at": (
                    record["first_archive_month"]
                    if anchor_basis == "first_observed_binance_futures_trade"
                    else None
                ),
                "delisting_evidence_state": delisting_state,
                "current_status_warning": (
                    None if is_trading else f"current_status_{status or 'archive_only'}"
                ),
                "onboard_start_discrepancy_seconds": difference,
                "onboard_start_discrepancy_status": discrepancy_status,
                "historical_inclusion_readiness": (
                    "ready" if ready else "excluded" if classification_complete and not in_research_scope else "blocked"
                ),
                "lifecycle_episode_count": len(intervals),
                "lifecycle_intervals": intervals,
            }
        )

        records.append(record)
    catalog = pd.DataFrame.from_records(records, columns=CATALOG_COLUMNS)
    return catalog.sort_values("symbol").reset_index(drop=True)


def catalog_coverage(catalog: pd.DataFrame) -> dict[str, Any]:
    """Return infrastructure coverage only; no scanner or outcome columns are accepted."""
    forbidden = {"hot_score", "hot_decile", "is_hot", "fwd_return_1d", "fwd_return_3d"}
    if forbidden & set(catalog.columns):
        raise ValueError("Lifecycle coverage cannot consume scanner or outcome data")
    usdt = catalog["quote_asset"].eq("USDT") | catalog["symbol"].str.endswith("USDT")
    current = catalog["present_in_current_exchange_info"].eq(True)
    resolved = catalog["scope_classification_complete"].eq(True)
    exact_listing = catalog["exact_official_trading_start_at"].notna()
    observed_anchor = catalog["eligibility_age_anchor_basis"].eq(
        "first_observed_binance_futures_trade"
    )
    legacy_anchor = catalog["eligibility_age_anchor_basis"].eq(
        "legacy_pre_research_start_adjudicated"
    )
    unresolved_anchor = catalog["eligibility_age_anchor_basis"].eq("unresolved")
    exact_delist_publication = catalog["delisting_announcement_published_at"].notna()
    exact_last_trading = catalog["official_last_trading_at"].notna()
    potentially_in_scope = usdt & catalog["scope_disposition"].isin(
        ["in_scope_crypto_perpetual", "benchmark_only", "unresolved"]
    )
    lifecycle_unresolved = potentially_in_scope & catalog[
        "historical_inclusion_readiness"
    ].eq("blocked")
    quarantined = lifecycle_unresolved

    def segment(mask: pd.Series) -> dict[str, int]:
        return {
            "symbols": int(mask.sum()),
            "exact_official_trading_start": int((mask & exact_listing).sum()),
            "first_observed_trade_anchor": int((mask & observed_anchor).sum()),
            "legacy_adjudicated_anchor": int((mask & legacy_anchor).sum()),
            "exact_delisting_announcement": int((mask & exact_delist_publication).sum()),
            "resolved_scope_classification": int((mask & resolved).sum()),
            "quarantined": int((mask & quarantined).sum()),
        }

    active = usdt & current & catalog["latest_known_status"].eq("TRADING")
    retained_non_trading = usdt & current & ~catalog["latest_known_status"].eq("TRADING")
    archive_only = usdt & ~current
    return {
        "catalog_schema_version": CATALOG_SCHEMA_VERSION,
        "total_archive_discovered_symbols": len(catalog),
        "total_usdt_candidate_symbols": int(usdt.sum()),
        "current_snapshot_symbols": int((usdt & current).sum()),
        "archive_only_symbols": int(archive_only.sum()),
        "exact_official_trading_start_coverage": int((usdt & exact_listing).sum()),
        "first_observed_trade_anchor_coverage": int((usdt & observed_anchor).sum()),
        "legacy_pre_research_start_adjudicated_coverage": int((usdt & legacy_anchor).sum()),
        "unresolved_age_anchor_coverage": int((potentially_in_scope & unresolved_anchor).sum()),
        "exchange_info_onboard_coverage": int(
            (usdt & catalog["exchange_info_onboard_at"].notna()).sum()
        ),
        "exact_official_delisting_announcement_coverage": int(
            (usdt & exact_delist_publication).sum()
        ),
        "exact_official_last_trading_coverage": int((usdt & exact_last_trading).sum()),
        "resolved_scope_classification_coverage": int((usdt & resolved).sum()),
        "unresolved_classification_cases": int((usdt & ~resolved).sum()),
        "unresolved_lifecycle_cases": int(lifecycle_unresolved.sum()),
        "currently_quarantined": int(quarantined.sum()),
        "historical_inclusion_ready": int(
            (potentially_in_scope & catalog["historical_inclusion_readiness"].eq("ready")).sum()
        ),
        "authorization_ready": not bool(quarantined.any()),
        "unresolved_categories": {
            "usdt_missing_exact_official_trading_start": int((usdt & ~exact_listing).sum()),
            "unresolved_age_anchor": int((potentially_in_scope & unresolved_anchor).sum()),
            "age_anchor_conflicts": int(
                (potentially_in_scope & catalog["age_anchor_conflict_status"].ne("none")).sum()
            ),
            "usdt_missing_scope_classification": int((usdt & ~resolved).sum()),
            "matched_listing_article_but_event_time_unresolved": int(
                (catalog["listing_article_id"].notna() & ~exact_listing).sum()
            ),
            "no_accepted_listing_article": int(catalog["listing_article_id"].isna().sum()),
            "archive_only_without_exact_delisting_announcement": int(
                (archive_only & ~exact_delist_publication).sum()
            ),
            "current_non_trading_without_exact_delisting_announcement": int(
                (retained_non_trading & ~exact_delist_publication).sum()
            ),
            "delisting_search_completed_no_reliable_timestamp": int(
                (
                    potentially_in_scope
                    & catalog["delisting_evidence_state"].eq(
                        "official_search_completed_no_reliable_announcement_timestamp"
                    )
                ).sum()
            ),
            "delisting_search_incomplete": int(
                (potentially_in_scope & catalog["delisting_evidence_state"].eq("not_investigated")).sum()
            ),
            "delisting_conflicts": int(
                (potentially_in_scope & catalog["delisting_evidence_state"].eq("conflicting_evidence")).sum()
            ),
            "stablecoin_classification_unresolved": int(
                (usdt & catalog["is_stablecoin_underlying"].isna()).sum()
            ),
            "leveraged_classification_unresolved": int(
                (usdt & catalog["is_leveraged_token"].isna()).sum()
            ),
            "stablecoin_classification_conflicts": int(
                (usdt & catalog["stablecoin_conflict_status"].eq("conflict")).sum()
            ),
            "leveraged_classification_conflicts": int(
                (usdt & catalog["leveraged_conflict_status"].eq("conflict")).sum()
            ),
            "unresolved_onboard_start_discrepancies": int(
                (
                    usdt
                    & catalog["onboard_start_discrepancy_status"].eq(
                        "unresolved_material_discrepancy"
                    )
                ).sum()
            ),
        },
        "segments": {
            "btc": segment(catalog["is_benchmark_btc"].eq(True)),
            "eth": segment(catalog["is_eth"].eq(True)),
            "active": segment(active),
            "current_retained_non_trading": segment(retained_non_trading),
            "delisted_or_archive_only": segment(archive_only),
        },
    }
