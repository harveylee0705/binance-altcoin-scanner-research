from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from alt_hot_scanner.identity import require_binance_token

CATALOG_SCHEMA_VERSION = "binance-usdm-lifecycle-v1"
PARSER_VERSION = "lifecycle-catalog-v1"

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
    "is_benchmark_btc",
    "is_eth",
    "scope_classification_status",
    "scope_classification_provenance",
    "listing_announcement_published_at",
    "official_trading_start_at",
    "exchange_info_onboard_at",
    "first_archive_month",
    "first_valid_kline_at",
    "delisting_announcement_published_at",
    "official_last_trading_at",
    "last_archive_month",
    "last_valid_kline_at",
    "exchange_info_delivery_at",
    "latest_known_status",
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
        "listing_evidence_status": "unresolved",
        "delisting_evidence_status": "unresolved",
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
    created = created_at or pd.Timestamp(datetime.now(UTC))
    current_by_symbol = (
        exchange_info_records.set_index("symbol", drop=False)
        if not exchange_info_records.empty
        else pd.DataFrame()
    )
    records: list[dict[str, Any]] = []
    for archive_row in archive_observations.sort_values("symbol").to_dict("records"):
        symbol = require_binance_token(archive_row["symbol"], "archive symbol")
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

        listing = _accepted_event(evidence, symbol, "listing")
        if listing is not None:
            record.update(
                {
                    "listing_announcement_published_at": listing["article_published_at"],
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
            # An exact match to an official New Cryptocurrency Listing article proves
            # crypto identity, but not stablecoin or leveraged-token absence.
            if record["is_crypto_underlying"] is None:
                record["is_crypto_underlying"] = True
                record["scope_classification_provenance"] = (
                    "official_binance_new_cryptocurrency_listing_article;"
                    "stablecoin_and_leveraged_status_unresolved"
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

        records.append(record)
    catalog = pd.DataFrame.from_records(records, columns=CATALOG_COLUMNS)
    return catalog.sort_values("symbol").reset_index(drop=True)


def catalog_coverage(catalog: pd.DataFrame) -> dict[str, Any]:
    """Return infrastructure coverage only; no scanner or outcome columns are accepted."""
    forbidden = {"hot_score", "hot_decile", "is_hot", "fwd_return_1d", "fwd_return_3d"}
    if forbidden & set(catalog.columns):
        raise ValueError("Lifecycle coverage cannot consume scanner or outcome data")
    usdt = catalog["quote_asset"].eq("USDT") | catalog["symbol"].str.endswith("USDT")
    current = catalog["metadata_acquired_at"].notna()
    resolved = catalog["scope_classification_status"].ne("unresolved")
    exact_listing = catalog["official_trading_start_at"].notna()
    exact_delist_publication = catalog["delisting_announcement_published_at"].notna()
    exact_last_trading = catalog["official_last_trading_at"].notna()
    lifecycle_unresolved = usdt & ~(exact_listing & (current | exact_delist_publication))
    quarantined = usdt & ~(resolved & exact_listing)

    def segment(mask: pd.Series) -> dict[str, int]:
        return {
            "symbols": int(mask.sum()),
            "exact_official_trading_start": int((mask & exact_listing).sum()),
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
        "unresolved_categories": {
            "usdt_missing_exact_official_trading_start": int((usdt & ~exact_listing).sum()),
            "usdt_missing_scope_classification": int((usdt & ~resolved).sum()),
            "matched_listing_article_but_event_time_unresolved": int(
                (catalog["listing_article_id"].notna() & ~exact_listing).sum()
            ),
            "no_accepted_listing_article": int(catalog["listing_article_id"].isna().sum()),
            "archive_only_without_exact_delisting_announcement": int(
                (archive_only & ~exact_delist_publication).sum()
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
