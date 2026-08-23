# Historical USD-M Contract Lifecycle Catalog

Schema version: `binance-usdm-lifecycle-v1`

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
The parser is versioned as `binance-announcement-v1`.

## Conservative matching and quarantine

Only exact canonical `<BASE>USDT` or exact `<BASE>/USDT` mentions match archive identities. Numeric
prefixes such as `1000...` are preserved. A single stated timestamp can apply to all exact symbols;
equal ordered symbol/timestamp counts map positionally. Other timestamp layouts remain unresolved.
Multiple official articles for the same symbol/event are marked ambiguous so relistings, migrations,
renames, and reused symbols cannot be silently collapsed. No fuzzy match is accepted automatically.

Archive-only symbols can gain crypto evidence from an exact official New Cryptocurrency Listing
article, but stablecoin and leveraged-token absence remain unknown unless separately established.
Unknown classification fails closed. Name suffixes such as `UP`/`DOWN` are never evidence, preserving
legitimate identities such as JUP and SYRUP.

## Eligibility contract

Eligibility requires resolved instrument scope, an exact official trading start, 30 elapsed calendar
days, valid market data, and a signal strictly before an exact known delisting-announcement publication
time. A null delisting announcement does not create an approximate cutoff. Current status never
back-filters history. Archive first/last months and valid-kline bounds never become lifecycle events.

Each run writes its catalog, evidence tables, coverage report, acquisition manifest, and unresolved
queue under ignored `reports/lifecycle/<RUN_ID>`, with raw evidence under
`data/raw/lifecycle/<RUN_ID>`. Review the unresolved queue and coverage gate before requesting any
full-history acquisition.
