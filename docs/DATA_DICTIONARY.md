# Data Dictionary

All timestamps are timezone-aware UTC. Physical Parquet partitioning may evolve without changing the
logical fields below. Nullable means “unknown/not computable,” never silently imputed.

## Contract metadata

| Field | Type | Meaning |
|---|---:|---|
| `symbol` | string | Binance contract symbol, e.g. `ETHUSDT` |
| `base_asset` | string | Contract underlying/base asset |
| `quote_asset` | string | Must be `USDT` for tradable v0.1 universe |
| `margin_asset` | string | Must be `USDT` |
| `contract_type` | string | Must be `PERPETUAL` |
| `market_family` | string | Must be canonical `USDM`; derived from the USD-M source endpoint |
| `product_family` | string | Must be canonical `FUTURES` |
| `underlying_type` | string | Binance classification; v0.1 crypto scope requires `COIN` |
| `underlying_subtype` | list[string] | Preserved Binance subtype tags |
| `is_leveraged_token` | bool | Explicit classification, never inferred from a name suffix |
| `classification_provenance` | string | Nonblank evidence/source; null means quarantine |
| `onboard_timestamp` | timestamp | Official API onboard/listing field where preserved |
| `first_valid_timestamp` | timestamp | First valid archived 1H open; observed lower data boundary |
| `delisting_announcement_timestamp` | timestamp? | Official announcement publication time only |
| `last_trading_timestamp` | timestamp? | Last valid/official trading boundary |
| `delivery_timestamp` | timestamp? | API delivery/delisting field when meaningful |
| `status` | string | Status observed at metadata acquisition, not historical state |
| `metadata_acquired_at` | timestamp | When metadata response was captured |
| `metadata_source` | string | URL/object and retrieval method |
| `is_eth` | bool | True only for ETHUSDT |
| `is_benchmark` | bool | True only for BTCUSDT |
| `is_later_delisted` | bool? | Derived only when evidence is available |

## Raw/normalized 1H bars

| Field | Type | Meaning |
|---|---:|---|
| `symbol` | string | Contract symbol |
| `open_time` | timestamp | Unique 1H UTC bar open |
| `open`, `high`, `low`, `close` | float64 | Contract prices |
| `base_volume` | float64 | Binance kline volume |
| `close_time` | timestamp | Binance close timestamp |
| `quote_volume` | float64 | Quote-asset turnover, primary liquidity measure |
| `trade_count` | int64 | Binance number of trades |
| `taker_buy_base_volume` | float64 | Preserved source field |
| `taker_buy_quote_volume` | float64 | Preserved source field |
| `source_key` | string | Immutable archive ZIP object key |
| `source_sha256` | string | Verified published checksum |

Primary key: (`symbol`, `open_time`). Duplicates are errors. OHLC must be positive, high must cover
open/close/low, volumes and trade count must be nonnegative, and timestamps must align exactly hourly.

## Completed 4H bars and feature rows

| Field | Meaning |
|---|---|
| `open_time`, `close_time` | Strict aggregate boundary and final underlying close time |
| `source_hour_count` | Must equal 4 |
| `continuity_segment` | Increments after a missing 4H boundary; rolling features never cross it |
| OHLC/volumes/trades | First/max/min/last/sum aggregation |
| `is_eligible` | Point-in-time universe and 30-day-age result |
| `contract_age_days` | Calendar elapsed time since official onboard timestamp |
| `return_1d`, `return_3d`, `return_7d` | 6/18/42-bar close returns |
| `rs_1d_pct`, `rs_3d_pct`, `rs_7d_pct` | Eligible-universe timestamp ranks |
| `vol_exp_4h_raw`, `vol_exp_4h_pct` | Current 4H volume / prior-30 median and rank |
| `current_24h_quote_volume` | Sum of last 6 completed 4H volumes |
| `vol_exp_24h_raw`, `vol_exp_24h_pct` | 24H volume / prior-20 rolling-24H median and rank |
| `median_30d_daily_quote_volume` | Rolling median of 180 completed rolling-24H observations |
| `liquidity_pct` | Cross-sectional rank of that continuous liquidity value |
| `prev_20d_high` | Prior 120-bar high; current excluded |
| `true_range`, `atr_14` | Wilder true range and ATR known at signal time |
| `distance_to_20d_high_atr` | `(prev_20d_high-close)/atr_14` |
| `is_fresh_breakout` | Current fresh close-above-prior-high event |
| `breakout_level`, `bars_since_breakout` | Most recent frozen breakout reference |
| `stage` | APPROACHING/TRIGGERED/EARLY_TREND/EXTENDED/OTHER |
| `hot_score` | Equal mean of five component percentiles |
| `hot_score_pct` | Timestamp-level cross-sectional HotScore rank |
| `hot_decile` | D1–D10 using deterministic percentile-to-decile mapping |
| `is_hot` | `hot_score_pct >= 0.9` |
| `episode_id`, `is_episode_start` | Independent HOT-state episode fields |

## Outcome fields

For suffix `1d`, `3d`, or `7d`: `fwd_return_*`, `btc_relative_return_*`, and
`cross_sectional_excess_return_*`. Excursions `mfe_3d`, `mae_3d`, `mfe_7d`, `mae_7d` exclude the
signal bar. `barrier_3d`/`barrier_7d` are one of `success`, `failure`, `censored`, `ambiguous`, or null
when the signal lacks ATR/full authorized data. `future_bar_count_*` makes censoring auditable.

## Acquisition manifest

Each object attempt records object key/URL, symbol, interval, period, retrieval timestamp, HTTP
result, bytes, published checksum, computed checksum, verification status, coverage, row count,
missing/duplicate counts, and error text. Raw ZIPs and metadata responses are append-only.

Download attempts additionally retain archive/checksum URLs, `retrieved_at`, `payload_source`,
parsed symbol, interval, period, HTTP/cache result, failure stage/URL, and every available published
or computed hash. Only a confirmed archive-object 404 is `missing`; checksum-sidecar absence and
verification mismatch are failures. Row coverage and missing-bar counts are added by the
normalization/processing quality manifest because they require reading the verified archive. An
occupied raw path is never overwritten; a newly published mismatch is preserved as a structured
failure so earlier manifest-to-byte lineage remains valid.

Object keys must exactly match
`data/futures/um/monthly/klines/<SYMBOL>/1h/<SYMBOL>-1h-<YYYY-MM>.zip`, with canonical uppercase
ASCII symbol identity. Manifest paths must equal the independently reconstructed canonical path
under `data/raw`. Before processing, the current Binance sidecar, manifest checksum evidence, and a
fresh SHA-256 of the local file must all agree. Attempt, slice, metadata, and quality manifests use
collision-resistant run IDs and exclusive no-replace creation.
