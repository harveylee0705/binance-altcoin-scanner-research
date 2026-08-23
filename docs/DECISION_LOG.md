# Decision Log

## 2026-08-23 — Freeze Scanner v0.1 before outcome inspection

Decision: encode the supplied universe, equal weights, windows, top-decile operational label,
structure tags, future outcomes, episodes, baselines, chronological splits, and pass criteria without
parameter search. Score scale is 0–1 and percentile ties use average ranks.

Reason: preserve the project as a falsifiable research experiment rather than an optimization loop.
Authorized evaluation in this kickoff: development-only engineering slice; no validation or final
holdout result inspection.

## 2026-08-23 — Strict completed-bar convention

Decision: 4H bars open at 00/04/08/12/16/20 UTC and require exactly four consecutive unique 1H bars.
Incomplete groups are rejected, never filled. The scanner is timestamped only after the final hour
has completed.

Reason: deterministic aggregation and prevention of partial-candle leakage.

## 2026-08-23 — Wilder ATR implementation

Decision: seed ATR(14) with the arithmetic mean of the first 14 true ranges, then apply Wilder's
recursive update. The first true range uses high-low because no prior close exists.

Reason: make “Wilder-style” exact and library-independent before outcomes are inspected.

## 2026-08-23 — Stage ambiguity resolution

Decision: use precedence TRIGGERED, EARLY_TREND, EXTENDED, APPROACHING, OTHER. Freeze the breakout
level at the triggering previous-20D high. EARLY_TREND means 1–3 bars after trigger; TRIGGERED owns
age zero. EXTENDED uses current signal-time ATR and the most recent known breakout level.

Reason: simplest deterministic interpretation consistent with the specification; these are tags,
not entries or score inputs.

## 2026-08-23 — Same-bar barrier crossings

Decision: label a future 4H bar touching both +2 and -1 signal-ATR barriers as `ambiguous`.

Reason: OHLC data cannot determine intrabar order; forcing either outcome would add untestable bias.

## 2026-08-23 — Official archive plus metadata overlay

Decision: use Binance Public Data monthly USD-M 1H archives and checksum sidecars as canonical OHLCV;
discover historical symbols from the archive index; overlay captured `/fapi/v1/exchangeInfo` metadata;
and use `/fapi/v1/fundingRate` only for separately stored future research.

Reason: archives are reproducible and include objects for later-delisted contracts, whereas a current
symbol list is not a historical point-in-time universe.

Known gap: no documented official bulk endpoint provides historical delisting announcement times.
They remain nullable and modular; last valid data is retained but not misrepresented as announcement
time.

## 2026-08-23 — Milestone review remediation cycle 1

Decision: make complete instrument classification a mandatory reusable-pipeline invariant, remove
the token-name suffix heuristic, and fail closed on missing classification fields. Require normalized
open and close timestamps to use UTC, require ordered symbol rows, and require each hourly close time
to equal open plus one hour minus one millisecond. Correct the full plan to end at the prior completed
month and persist the complete published/computed checksum lineage in full-download attempts.

Reason: independent review found that callers could bypass scope filtering, legitimate JUP/SYRUP
contracts were excluded by suffix, and malformed/non-UTC close times could affect completed-bar and
listing-age logic. No scanner outcomes informed these changes; they correct specification-enforcement
and auditability defects.
