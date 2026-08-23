from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import numpy as np
import pandas as pd

from alt_hot_scanner.identity import require_archive_symbol_identity

CATALOG_SCHEMA_VERSION = "binance-usdm-lifecycle-v3"
PARSER_VERSION = "lifecycle-catalog-v3"

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


def build_lifecycle_catalog(
    archive_observations: pd.DataFrame,
    exchange_info_records: pd.DataFrame,
    announcement_evidence: pd.DataFrame | None = None,
    *,
    created_at: pd.Timestamp | None = None,
    announcement_search_completed: bool | None = None,
    first_observed_trades: pd.DataFrame | None = None,
    scope_registry_records: list[dict[str, Any]] | None = None,
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
    scope_by_symbol = {
        row.get("contract_identity"): row for row in (scope_registry_records or [])
    }
    if len(scope_by_symbol) != len(scope_registry_records or []):
        raise ValueError("Scope registry records contain duplicate identities")
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
        delisting = _accepted_event(evidence, symbol, "delisting")
        if delisting is not None:
            record.update(
                {
                    "delisting_announcement_published_at": delisting[
                        "article_published_at"
                    ],
                    "official_last_trading_at": delisting["official_event_at"],
                    "delisting_evidence_status": delisting["event_time_evidence_status"],
                    "delisting_source_type": "official_binance_structured_announcement",
                    "delisting_source_url": delisting["source_url"],
                    "delisting_article_id": delisting["article_code"],
                    "delisting_retrieved_at": delisting["retrieved_at"],
                    "delisting_raw_snapshot_path": delisting["raw_snapshot_path"],
                    "delisting_raw_snapshot_sha256": delisting["raw_snapshot_sha256"],
                    "delisting_parser_version": delisting["parser_version"],
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
        elif exact_start is not None:
            anchor = exact_start
            anchor_basis = "exact_official_original_launch"
        elif observed_start is not None:
            observed_timestamp = pd.Timestamp(observed_start)
            if observed_timestamp <= pd.Timestamp("2019-12-02T00:00:00Z"):
                anchor = observed_start
                anchor_basis = "legacy_pre_research_start_adjudicated"
            elif observed_timestamp < pd.Timestamp("2020-01-01T00:00:00Z"):
                # The verified trade explicitly adjudicates that the contract was not yet
                # 30 days old at research start; it becomes eligible 30 days after this
                # conservative boundary rather than being guessed older.
                anchor = observed_start
                anchor_basis = "first_observed_binance_futures_trade"
            else:
                anchor = observed_start
                anchor_basis = "first_observed_binance_futures_trade"
        listing_complete = anchor is not None and anchor_conflict == "none"
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
        onboard = record["exchange_info_onboard_at"]
        official = record["official_trading_start_at"]
        if onboard is not None and official is not None:
            difference = abs(
                (pd.Timestamp(official) - pd.Timestamp(onboard)).total_seconds()
            )
            discrepancy_status = (
                "consistent_within_engineering_threshold"
                if difference <= 3600
                else "unresolved_material_discrepancy"
            )
        else:
            difference = None
            discrepancy_status = "not_comparable"
        delisting_ready = not has_delisting_conflict and (
            is_trading
            or exact_delisting
            or delisting_state
            == "official_search_completed_no_reliable_announcement_timestamp"
        )
        in_research_scope = record["scope_disposition"] in {
            "in_scope_crypto_perpetual",
            "benchmark_only",
        }
        ready = (
            classification_complete
            and in_research_scope
            and listing_complete
            and delisting_ready
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
