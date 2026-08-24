# Bias, Data Quality, and Limitations

## Historical lifecycle evidence

Binance's current `exchangeInfo` endpoint describes current/retained symbol metadata; it is **not** a
historical snapshot service. Using its current symbols alone would omit some delisted contracts and
create survivorship bias. Scanner v0.1 therefore discovers the historical symbol set from the
official public archive object index, then joins separately captured metadata.

The archive reliably establishes that data exists and provides first/last valid observations. It
does not, by itself, prove the exact futures listing announcement time or the time a delisting became
public. Binance does expose a publicly accessible structured CMS catalog/detail mechanism used by
its announcement pages. The lifecycle builder captures catalog 48 (New Cryptocurrency Listing) and
catalog 161 (Delisting), stores raw JSON responses immutably with SHA-256, and conservatively matches
exact canonical symbols. This is not part of the documented Futures market-data API, so its coverage
and reproducibility are audited on every run.

Where exact launch evidence is unavailable:

- retain delisted archives and last valid/trading timestamps;
- never fabricate `delisting_announcement_published_at`;
- set that field null with provenance;
- use only the earliest checksum-verified official Futures trade as a later, conservative age
  boundary and report the resulting early-history coverage loss;
- never call that observed boundary an exact listing time;
- after a comprehensive official search, permit a null delisting announcement as the explicit
  `official_search_completed_no_reliable_announcement_timestamp` state.

Search engines and third-party pages may locate an official article but never become the stored
authority. Multiple official articles for one symbol/event are marked ambiguous instead of silently
collapsed; this is also the relisting/reused-symbol safeguard.

Genuine termination/relisting evidence is represented as multiple non-overlapping lifecycle
intervals. No scanner row is eligible in the terminated gap, and a relisted episode must accumulate
30 full calendar days from its own conservative live boundary. Articles for Spot, Margin, Delivery,
or COIN-margined products cannot terminate a USD-M perpetual merely because the body mentions the
same asset.

## Leakage controls

- Features are grouped by symbol, sorted by time, and use only current/prior completed data.
- Rolling baselines explicitly shift before calculating historical medians/highs.
- The current 4H bar is excluded from its volume baseline and previous-20D high.
- Cross-sectional ranks contain only point-in-time eligible contracts.
- Current status/liquidity cannot back-filter historical rows.
- Future labels are computed after feature construction and cannot feed scanner columns.
- Split guards default to development; labels at a split edge require the full horizon inside it.

## Missing and inconsistent OHLCV

No price or volume is filled. A 4H group with fewer/more than four unique consecutive 1H bars is
rejected and counted. Duplicates, invalid OHLC relationships, negative volume, non-hour boundaries,
checksum mismatch, and conflicting archive versions are hard errors requiring investigation. Monthly
archives are retained exactly; archive updates can change checksums, so acquisition time and checksum
must accompany reproducibility claims.
If a published checksum changes, the existing raw object is not replaced. The acquisition fails with
both hashes recorded; later work may add a separate content-addressed/versioned raw object without
mutating the original path. Missing checksum sidecars are verification failures, not missing market
data, unless a separate archive-object probe confirms a 404.
Remote bytes are first written to an isolated temporary file and independently hashed before an
atomic no-replace install. Canonical paths are derived only from the strict Binance USD-M monthly 1H
object-key grammar and are resolved under the configured raw root. Processing does not trust a prior
`verified` flag: it validates each manifest entry, re-fetches the exact named checksum sidecar, and
re-hashes current local bytes immediately before parsing. Manifest and metadata writes use exclusive,
collision-resistant no-replace installation.
Rolling features reset after a missing 4H boundary, outcome labels require an exact gap-free elapsed
horizon, and a later HOT row after a gap starts a new episode.

## Listing and delisting boundaries

The separate `eligibility_age_anchor_at` is either an exact official original launch, a conservative
checksum-verified first Futures trade, or an explicitly adjudicated legacy boundary. Current
`exchangeInfo.onboardDate` is retained separately and discrepancies are warnings; it is not silently
renamed as an exact launch. The 30-day minimum age is fixed and does not change based on outcomes.
`deliveryDate` may be a far-future sentinel for active perpetuals and must not be interpreted as an
actual delisting. Current active/delisted status never removes historical rows.

## Approval and acquisition boundary

The approval-time full replay binds and re-hashes raw archive XML, exchangeInfo, CMS catalog/detail
JSON, provenance sidecars, first-trade ZIPs, and reviewed daily boundary indexes. Runtime download
validation consumes the approved hashes and content identities rather than repeating that entire
research replay for every OHLCV object. An approval can be active, revoked, or superseded; only the
active state authorizes a plan. No real full-history approval is created by lifecycle construction.

Monthly archives stop at the last fully completed UTC month. The current-month tail is deliberately
not implemented in this milestone, so monthly data alone cannot claim the latest fully completed
2026 bars during an in-progress month.

## Contract classification

Filtering only by a `USDT` suffix is insufficient. Full processing must require quote/margin assets
and `PERPETUAL`, exclude stablecoin and leveraged-token underlyings, and reject TradFi/non-crypto
products based on captured metadata. Classification uses a completed finite-universe audit bound to
the exact archive candidate-set hash. Positive stablecoin, leveraged-token, TradFi, index, composite,
and delivery exclusions retain provenance. Only candidates in that exact audited set may receive
reviewed negative classifications; a changed candidate set invalidates completeness automatically.

The reusable pipeline enforces this classification centrally. Stablecoin admission depends on the
captured per-contract subtype evidence, not whether a base appears in the legacy configuration list.
Leveraged-token status comes from
explicit metadata/provenance; suffix matching is prohibited because it misclassifies legitimate
assets such as JUP and SYRUP.
Missing/null/empty/malformed underlying-subtype evidence or blank identity/provenance fields are
quarantined. This is intentionally stricter than treating absence of a leveraged tag as proof of a
normal crypto underlying.
All classification-bearing identities pass a canonical semantic boundary before scope decisions.
ASCII Binance tokens retain their strict grammar. Genuine non-ASCII exchange identities retain exact
NFC text while local paths use collision-resistant ASCII components; traversal characters and
control characters remain prohibited. Stablecoin, BTC, and ETH decisions therefore operate only on
validated identities.

## Statistical dependence and inference

Overlapping 24H volume observations and forward horizons create serial dependence. Every 4H eligible
row is kept for cross-sectional deciles, while independent HOT transitions form the primary event
dataset. Inference must use dependence-aware methods (for example time/block bootstrap or clustered
errors) in a later analysis milestone; v0.1 feature definitions are not altered to make samples look
independent.

## Intrabar path ambiguity

When both +2 ATR and -1 ATR fall within one future 4H high-low range, 4H OHLC cannot reveal which was
first. The label is `ambiguous`; it is not assigned optimistically or pessimistically. A later study
may resolve these events with lower-timeframe data under an explicitly versioned rule.

## Vertical-slice limits

The kickoff slice is a development-only engineering verification over five symbols and a short 2023
window. It cannot establish scanner edge, regime robustness, delisted-contract completeness, or pass
criteria. Its output must not be presented as a backtest or scanner result.
