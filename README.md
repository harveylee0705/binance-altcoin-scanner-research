# Binance Altcoin Hot Pick Scanner

This repository is the research foundation for Scanner v0.1. Its only current question is:

> Does a simple, equal-weight HotScore identify Binance USD-M USDT perpetual altcoins that
> subsequently outperform?

It does **not** implement a trading strategy. Volume Profile, entries, leverage, stops, sizing,
pyramiding, exits, portfolio PnL, optimization, and machine learning are explicitly out of scope.

## Frozen design

The scanner operates after completed UTC 4-hour bars (opens at 00, 04, 08, 12, 16, and 20 UTC),
which are built strictly from four consecutive 1-hour Binance USD-M bars. Eligible contracts are
USDT-margined perpetuals with at least 30 full calendar days of futures history. BTC is retained as
the benchmark but is never tradable; ETH is tradable and carries an `is_eth` flag. There is no hard
liquidity filter.

HotScore is the unweighted mean of the cross-sectional percentiles of 1D, 3D, and 7D return,
4H volume expansion, and 24H volume expansion. Price structure is a tag only. Full definitions,
research sequencing, pre-registered pass criteria, and limitations are in
[`docs/RESEARCH_SPEC_V0_1.md`](docs/RESEARCH_SPEC_V0_1.md).

## Data sources

- Canonical OHLCV: Binance Public Data monthly USD-M archive, including `.CHECKSUM` sidecars.
- Current contract metadata: `GET /fapi/v1/exchangeInfo` (useful fields include `onboardDate`,
  `deliveryDate`, `contractType`, assets, and status).
- Historical symbol discovery: the public archive object index, not the current exchange list.
- Funding: `GET /fapi/v1/fundingRate`, stored for future work but excluded from v0.1 features.
- Delisting announcements: official Binance announcement pages require a separately versioned,
  auditable reconstruction; no timestamp is inferred when unavailable.

The archive's first valid kline is an observed data boundary, not proof of the public announcement
time. Current `exchangeInfo` is not a historical point-in-time membership source. See
[`docs/BIAS_AND_LIMITATIONS.md`](docs/BIAS_AND_LIMITATIONS.md).

## Setup

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -e ".[dev]"
```

## Reproduce the development-only vertical slice

The following command downloads only five symbols and four monthly files per symbol from March
through June 2023. It verifies the published checksum, preserves ZIPs under `data/raw`, normalizes
1H data, creates strict 4H bars, applies point-in-time eligibility, builds features and HotScore,
and labels outcomes only inside the development split.

```powershell
.venv\Scripts\python scripts/run_vertical_slice.py --config config/research_v0_1.yaml
```

It writes immutable-source manifests under `data/raw`, normalized/processed Parquet under ignored
data directories, and an audit summary under `reports`. It deliberately does not print or analyze
performance by score decile. Re-running skips matching verified raw files.

To prepare a scalable download manifest without downloading full history:

```powershell
.venv\Scripts\python scripts/prepare_full_manifest.py --config config/research_v0_1.yaml
```

This discovers archived symbols (including delisted ones) from the object index and emits expected
monthly 1H object keys. It is a planning artifact; missing objects must be logged, not filled.

The prepared full-data path is resumable and deliberately requires an execution switch:

```powershell
# Dry-run validation only (safe default)
.venv\Scripts\python scripts/download_from_plan.py

# Future explicit execution, after the historical contract catalog is approved
.venv\Scripts\python scripts/download_from_plan.py --execute --workers 4
.venv\Scripts\python scripts/process_downloaded_archives.py `
  --attempt-manifest data/raw/manifests/full_download_attempts_<RUN_ID>.json
```

The downloader verifies every checksum and writes successes, missing objects, and failures to an
append-only attempt manifest. Processing is per symbol, rejects duplicates/incomplete 4H groups, and
writes partitioned Parquet, avoiding an all-history in-memory normalization step. Do not execute the
full plan until historical instrument classification and lifecycle metadata are resolved.

## Tests and quality checks

```powershell
.venv\Scripts\python -m pytest
.venv\Scripts\ruff check .
```

Tests cover strict aggregation, eligibility and age, frozen feature windows/baselines, ranking,
ATR/high construction, episodes, forward alignment, barrier labels, split guards, and an end-to-end
synthetic slice. All timestamps are timezone-aware UTC. Missing OHLCV is never silently filled.

## Research split guard

Normal analysis code allows only `development` by default. Access to validation must be explicit
after the scanner is frozen. Final holdout access requires a separately explicit opt-in and must not
occur during scanner development. The kickoff slice is hard-bounded to end on 2023-06-20 so all 7D
labels remain within development.
