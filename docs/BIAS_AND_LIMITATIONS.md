# Bias, Data Quality, and Limitations

## Known blocking data-source limitation

Binance's current `exchangeInfo` endpoint describes current/retained symbol metadata; it is **not** a
historical snapshot service. Using its current symbols alone would omit some delisted contracts and
create survivorship bias. Scanner v0.1 therefore discovers the historical symbol set from the
official public archive object index, then joins separately captured metadata.

The archive reliably establishes that data exists and provides first/last valid observations. It
does not, by itself, prove the exact futures listing announcement time or the time a delisting became
public. Historical official announcement timestamps are not exposed by a documented bulk market-
data endpoint. Until an auditable official announcement corpus is reconstructed:

- retain delisted archives and last valid/trading timestamps;
- never fabricate `delisting_announcement_timestamp`;
- set that field null with provenance;
- disclose that new events between public announcement and last trading may remain eligible;
- keep the eligibility interface able to enforce the timestamp when later supplied.

This limitation does not invalidate the engineering slice, but full historical inference must report
its possible bias and ideally complete the announcement catalog before scanner evaluation.

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

Official `onboardDate` is preferred for eligibility. First valid archived data is a quality field and
fallback candidate requiring an explicit provenance flag; it must not silently replace known listing
metadata. The 30-day minimum age is fixed and does not change based on outcomes. `deliveryDate` may be
a far-future sentinel for active perpetuals and must not be interpreted as an actual delisting.

## Contract classification

Filtering only by a `USDT` suffix is insufficient. Full processing must require quote/margin assets
and `PERPETUAL`, exclude stablecoin and leveraged-token underlyings, and reject TradFi/non-crypto
products based on captured metadata. Historical archive symbols missing authoritative classification
remain quarantined rather than guessed into the universe.

The reusable pipeline enforces this classification centrally. Leveraged-token status comes from
explicit metadata/provenance; suffix matching is prohibited because it misclassifies legitimate
assets such as JUP and SYRUP.
Missing/null/empty/malformed underlying-subtype evidence or blank identity/provenance fields are
quarantined. This is intentionally stricter than treating absence of a leveraged tag as proof of a
normal crypto underlying.
All classification-bearing symbols, assets, product types, subtype labels, and provenance values
pass one canonical identity boundary before scope decisions. Wrong types, padding, control
characters, non-ASCII/confusable encodings, noncanonical casing for Binance tokens, and inconsistent
symbol/base/quote identities are quarantined rather than trimmed, coerced, or recased. Stablecoin,
BTC, and ETH decisions therefore operate only on validated canonical identities.

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
