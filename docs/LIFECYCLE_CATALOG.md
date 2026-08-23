# Historical USD-M Contract Lifecycle Catalog

Schema version: `binance-usdm-lifecycle-v2`

The catalog is research infrastructure, not a scanner result. Build it with:

```powershell
.venv\Scripts\python scripts/build_lifecycle_catalog.py
```

This performs metadata-only acquisition. It does not download monthly OHLCV archives, compute
HotScore, label outcomes, or read validation/holdout scanner observations.

## Evidence layers

1. The official Binance public S3 `ListObjectsV2` index discovers every historical USD-M monthly
   kline symbol and each symbol's first/last observed 1H archive month. Every page is followed through
   `NextContinuationToken`; raw XML, URL, retrieval time, and SHA-256 are retained. These fields prove
   observed data existence only.
2. A current official `/fapi/v1/exchangeInfo` snapshot supplies current identity, product, subtype,
   onboard, delivery, and status metadata. It is never treated as historical universe membership.
3. Binance's public structured CMS list/detail responses supply listing/delisting articles. Catalog
   pages and article details are stored as immutable raw JSON. Article publication time remains
   separate from stated trading-start or last-trading time.

The CMS acquisition uses these publicly accessible endpoints:

- `.../bapi/composite/v1/public/cms/article/list/query`
- `.../bapi/composite/v1/public/cms/article/detail/query`

No authentication, browser automation, anti-bot bypass, or access-control circumvention is used.
The parser is versioned as `binance-announcement-semantic-v2`.

## Conservative matching and quarantine

Exact canonical `<BASE>USDT` or exact `<BASE>/USDT` identity is necessary but not sufficient. Each
article is first assigned a positive product semantic class. Only an original perpetual-contract
launch can supply a listing start, and only an applicable Futures delisting/settlement article can
supply a delisting cutoff. Copy Trading, bot, portfolio-margin/Multi-Assets, pre-market, parameter,
maintenance, ambiguous, and irrelevant articles cannot establish an original listing.

An event time is accepted only when the article contains an explicit action-symbol-time structure.
One explicit launch time may apply to multiple symbols in the same launch statement, and structured
rows may supply distinct times. Timestamps are never assigned by first occurrence, proximity alone,
or equal symbol/timestamp counts. Ambiguous layouts and conflicting applicable articles remain
unresolved.

Stablecoin and leveraged-token classification is tri-state. Positive official subtype evidence and
the frozen known-stablecoin guard can establish `True`; absence of a subtype never establishes
`False`. A `False` value requires accepted versioned reviewed evidence, conflicts stay explicit, and
unknown classification fails closed. Name suffixes such as `UP`/`DOWN` are never evidence.

## Eligibility contract

Eligibility requires resolved instrument scope, an exact official trading start, 30 elapsed calendar
days, valid market data, and a signal strictly before an exact known delisting-announcement publication
time. A null delisting announcement does not create an approximate cutoff. Current status never
back-filters history. Archive first/last months and valid-kline bounds never become lifecycle events.

Each run writes its catalog, evidence tables, recomputed readiness report, lifecycle bundle,
coverage report, acquisition manifest, and unresolved queue under ignored
`reports/lifecycle/<RUN_ID>`. The bundle SHA-256-binds the exact catalog, classification,
announcement, archive, coverage, readiness, noncanonical queue, and frozen config. New raw snapshots
receive immutable provenance sidecars; legacy cache entries without recoverable acquisition evidence
remain retrieval-time unresolved.

Full-history planning uses only actual ZIP keys observed in the preserved paginated archive index.
The planner and downloader re-verify the bundle, artifact hashes, config digest, recomputed readiness,
exact plan schema, and exact observed object set. A checksum sidecar alone is not archive evidence.

Monthly archives end at the last fully completed UTC calendar month. The repository does not yet
implement a separately verified daily-archive or API tail, so it cannot claim the latest fully
completed available 2026 bars during an in-progress month. That tail is a pre-evaluation acquisition
blocker and is not implemented by this lifecycle remediation.
