# Historical USD-M Contract Lifecycle Catalog

Schema version: `binance-usdm-lifecycle-v3`

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
3. The earliest available official USD-M daily trade archive for each reviewed in-scope identity is
   downloaded once, verified against its official SHA-256 sidecar, and parsed for the strict integer
   minimum trade timestamp. This is `first_observed_trade_at`, never an exact launch timestamp.
4. Binance's public structured CMS list/detail responses supply listing/delisting articles. Catalog
   pages and article details are stored as immutable raw JSON. Article publication time remains
   separate from stated trading-start or last-trading time.

The CMS acquisition uses these publicly accessible endpoints:

- `.../bapi/composite/v1/public/cms/article/list/query`
- `.../bapi/composite/v1/public/cms/article/detail/query`

No authentication, browser automation, anti-bot bypass, or access-control circumvention is used.
The parser is versioned as `binance-announcement-semantic-v3`. Every article in the Futures
delisting/settlement catalog is inspected; delisting acquisition is not title-filtered on `delist`.

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

Stablecoin, leveraged-token, and product classification is bound to a versioned finite-universe
registry containing the exact canonical candidate-set SHA-256. Positive exclusions retain evidence.
For that exact completed audit only, remaining identities may receive
`reviewed_finite_universe_negative`; any new candidate invalidates the registry. Name suffixes such
as `UP`/`DOWN` are discovery flags only and never classification evidence.

## Eligibility contract

Eligibility requires resolved instrument scope, a resolved eligibility-age anchor, 30 elapsed
calendar days, valid market data, and a signal strictly before an exact known delisting-announcement
publication time. Exact official original launch is preferred. Otherwise a checksum-verified first
Binance Futures trade is a conservative live boundary and is never relabeled exact. A completed
official delisting search with no reliable publication timestamp leaves the cutoff null and does not
remove historical data. Incomplete/conflicting evidence fails closed. Current status never
back-filters history.

Each run writes its catalog, evidence tables, recomputed readiness report, lifecycle bundle,
coverage report, acquisition manifest, and unresolved queue under ignored
`reports/lifecycle/<RUN_ID>`. The bundle SHA-256-binds the exact catalog, classification,
announcement, archive, coverage, readiness, noncanonical queue, and frozen config. New raw snapshots
receive immutable provenance sidecars; legacy cache entries without recoverable acquisition evidence
remain retrieval-time unresolved.

Full-history planning uses only actual ZIP keys observed in the preserved paginated archive index.
Checkpoint resume confines and re-hashes every raw XML page, verifies available provenance sidecars,
reparses ZIP identities/bounds, and compares reconstructed primitives with checkpoint fields. Legacy
snapshots without recoverable acquisition time retain a null time.

The planner has no implicit newest-bundle mode. It requires exact `--bundle` and `--approval` paths.
The approval pin binds the bundle ID, lifecycle code commit, config digest, and independent-review
artifact hash. No production approval is generated by the lifecycle builder. The integrity threat
model covers stale/wrong bundles, changed bytes, corrupt evidence, mismatched config/code, forged
simple plans, and superseded review artifacts. It intentionally does not attempt to defeat an
operator with full Git/filesystem write access who fabricates every primitive and recomputes hashes;
SHA-256 integrity plus explicit reviewed-bundle pinning is sufficient for this local harness.

Monthly archives end at the last fully completed UTC calendar month. The repository does not yet
implement the current-month data tail, so it cannot claim the latest fully completed available 2026
bars during an in-progress month. A later acquisition phase must combine completed monthly archives,
non-overlapping daily archives after the monthly boundary, and if needed a small API tail. That work
is not part of this lifecycle remediation.
