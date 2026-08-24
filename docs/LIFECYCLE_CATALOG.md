# Historical USD-M Contract Lifecycle Catalog

Schema version: `binance-usdm-lifecycle-v5`

The catalog is research infrastructure, not a scanner result. Build it with:

```powershell
.venv\Scripts\python scripts/build_lifecycle_catalog.py `
  --scope-registry config/reviewed_scope_registry_b323b3c3.json `
  --adjudications config/lifecycle_adjudications_v1.json `
  --delisting-registry config/historical_delisting_cutoff_registry.json `
  --delisting-review docs/reviews/delisting_registry_independent_review_2026-08-24.json
```

This performs metadata-only acquisition. It does not download monthly OHLCV archives, compute
HotScore, label outcomes, or read validation/holdout scanner observations.

## Reviewed scope boundary

Archive discovery first produces a deterministic candidate inventory and `candidate_set_digest`.
The normal builder then requires a separately maintained `historical-scope-registry-v2` whose exact
candidate identities and digest match. It never manufactures review dispositions or finite-universe
negatives. A mismatch stops the build and emits a review-required difference artifact.

The reviewed registry records USDT/perpetual/crypto applicability, stablecoin and leveraged-token
exclusions, noncrypto/index/composite dispositions, and the BTC benchmark. Candidate-bound reviewed
negatives are valid for that finite universe. Every positive stablecoin/leveraged exclusion requires
direct evidence, and the registry itself binds an independently hashed PASS artifact.

## Evidence layers

1. The official Binance public S3 `ListObjectsV2` index discovers every historical USD-M monthly
   kline symbol and each symbol's first/last observed 1H archive month. Every page is followed through
   `NextContinuationToken`; raw XML, URL, retrieval time, and SHA-256 are retained. These fields prove
   observed data existence only.
2. A current official `/fapi/v1/exchangeInfo` snapshot supplies current identity, product, subtype,
   onboard, delivery, and status metadata. It is never treated as historical universe membership.
3. The earliest applicable official USD-M daily trade archive for every reviewed lifecycle episode
   is downloaded once, verified against its official SHA-256 sidecar, and parsed for the strict
   integer minimum trade timestamp. This controls `eligibility_age_anchor_at`; exact listing and
   relisting timestamps remain separately stored descriptive metadata.
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

Eligibility requires resolved instrument scope, a checksum-verified first-trade eligibility anchor,
30 elapsed calendar days, valid market data, and a signal strictly before an exact reviewed
delisting-announcement publication time. Listing announcements never control age eligibility. A completed
official delisting search with no reliable publication timestamp leaves the cutoff null and does not
remove historical data. Incomplete/conflicting evidence fails closed. Current status never
back-filters history.

When evidence establishes `live → terminated → relisted`, `lifecycle_intervals` contains ordered,
non-overlapping episodes. Eligibility selects the interval active at the signal timestamp, blocks
timestamps before the first episode, after termination, and inside gaps, and resets the fixed
30-calendar-day age clock from the first verified post-gap trade of each relisted episode. A
current/old-evidence disagreement alone is not enough to create an episode.

The immutable reviewed delisting registry is the sole authority for lifecycle-critical publication
cutoffs. The approval-time eligibility oracle independently reconstructs episode eligibility from
primitive trade evidence and reviewed registries. Its module does not import or call the production
announcement parser or lifecycle builder; only after derivation does it compare against the catalog.

Each run writes its catalog, evidence tables, candidate inventory, reviewed-registry copy,
adjudications, daily boundary evidence, primitive manifest, independent oracle report, recomputed readiness
report, lifecycle bundle,
coverage report, acquisition manifest, and unresolved queue under ignored
`reports/lifecycle/<RUN_ID>`. The bundle SHA-256-binds the exact catalog, classification,
announcement, archive, scope review, adjudications, replay, coverage, readiness, noncanonical queue,
and frozen config. New raw snapshots
receive immutable provenance sidecars; legacy cache entries without recoverable acquisition evidence
remain retrieval-time unresolved.

Full-history planning uses only actual ZIP keys observed in the preserved paginated archive index.
Checkpoint resume confines and re-hashes every raw XML page, verifies available provenance sidecars,
reparses ZIP identities/bounds, and compares reconstructed primitives with checkpoint fields. Legacy
snapshots without recoverable acquisition time retain a null time.

The review-time command `scripts/verify_lifecycle_evidence_full.py` re-hashes every bound primitive,
reconstructs candidates and observed monthly keys from XML, reparses exchangeInfo and CMS evidence,
re-hashes/reparses the local first-trade ZIPs, checks boundary indexes, rebuilds the catalog, and
requires exact derived-state equality. This expensive replay runs for lifecycle approval, not before
each future OHLCV object.

The planner has no implicit newest-bundle mode. It requires exact `--bundle` and `--approval` paths.
The approval pin binds the bundle ID, lifecycle code commit, config digest, and independent-review
artifact hash. A separate content-bound state registry marks it active, revoked, or superseded;
only active authorizes planning or execution. Runtime verifies the approved bundle and all artifact
hashes, including primitive manifest, replay report, scope registry/review, config, plan integrity,
and executable commit. No production approval is generated by the lifecycle builder. The integrity
threat model covers stale/wrong bundles, changed bytes, corrupt evidence, mismatched config/code,
forged simple plans, and superseded approvals. It intentionally does not attempt to defeat an
operator with full Git/filesystem write access who fabricates every primitive and recomputes hashes;
SHA-256 integrity plus explicit reviewed-bundle pinning is sufficient for this local harness.

Monthly archives end at the last fully completed UTC calendar month. The repository does not yet
implement the current-month data tail, so it cannot claim the latest fully completed available 2026
bars during an in-progress month. A later acquisition phase must combine completed monthly archives,
non-overlapping daily archives after the monthly boundary, and if needed a small API tail. That work
is not part of this lifecycle remediation.
