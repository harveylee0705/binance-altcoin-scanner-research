# Decision Log

## 2026-08-23 — Pre-evaluation lifecycle age-anchor evidence policy

Decision: a contract must never become eligible before it existed and traded on Binance Futures,
and eligibility continues to require at least 30 calendar days of age. The eligibility-age anchor is
now a separate lifecycle fact with an explicit basis. A reliable official source that positively
establishes the exact original launch of the exact perpetual contract is recorded as
`exact_official_trading_start_at` with basis `exact_official_original_launch`. When that evidence is
unavailable, the earliest independently checksum-verified Binance USD-M Futures trade timestamp may
be recorded as `first_observed_trade_at` and used with basis
`first_observed_binance_futures_trade`. It must not be relabeled as an exact official listing time.
Legacy contracts already known to be trading at research start require explicit official evidence
that they existed by 2019-12-02 before receiving basis `legacy_pre_research_start_adjudicated`;
otherwise the basis remains `unresolved`.

Decision: the fixed 30-calendar-day rule operates on `eligibility_age_anchor_at`. A later observed
trade boundary can conservatively exclude early history and that coverage loss must be reported. No
earlier boundary may be inferred. Exact launch coverage, conservative observed-trade coverage,
legacy adjudication, and unresolved anchors must be reported separately.

Reason: a verified market trade proves the contract was live by that instant without asserting an
unavailable exact launch time. This conservatively preserves the no-prelisting invariant while
avoiding an unjustified requirement for perfect historical announcement coverage. This is an
evidence-resolution policy recorded before scanner outcome inspection, not a scanner parameter
change. No scanner performance, validation-period results, or final-holdout results were inspected.

## 2026-08-23 — Pre-evaluation correction: HOT is canonical D10 membership

Decision: define `is_hot` solely as `hot_decile == 10`, retaining average-tie percentile ranks,
equal HotScore weights, and D1–D10 as the primary analysis. Ties receive their average-rank decile;
there is no forced HOT member in a small cross-section and a boundary tie may make D10 empty or
larger than exactly ten percent.

Reason: the independent `hot_score_pct >= 0.9` translation selected ranks at both 0.9 and 1.0, so
ten uniquely ranked contracts produced two HOT members. This is a pre-evaluation implementation/spec
translation correction; no scanner outcomes were inspected and no research parameter changed.

## 2026-08-23 — Pre-evaluation correction: complete archive-index pagination

Decision: consume every S3 `ListObjectsV2` page through explicit continuation tokens, validate the
XML and canonical prefix/key grammar, reject missing/repeated/inconsistent tokens, and retain page,
prefix, uniqueness, and truncation audit counts.

Reason: a single response is not proof of complete historical-symbol discovery. This is a
pre-evaluation acquisition-integrity correction made without downloading full OHLCV or inspecting
scanner outcomes.

## 2026-08-23 — Pre-evaluation correction: enforce normalized OHLCV structure

Decision: normalized 1H validation rejects non-finite retained numeric fields, non-positive OHLC,
impossible high/low relationships, negative base/quote/taker volumes, and negative trade counts.
Internally consistent extreme observations remain valid.

Reason: implementation did not fully enforce the already documented hard-error contract. This is a
pre-evaluation specification-enforcement correction, not a market-data cleaning or strategy change.

## 2026-08-23 — Historical lifecycle evidence hierarchy and quarantine gate

Decision: version the catalog as `binance-usdm-lifecycle-v1`; keep exact official announcement
publication/trading timestamps, current-snapshot onboard/delivery/status, archive-observed month
bounds, and valid-kline bounds as distinct nullable evidence. Acquire official structured Binance CMS
catalog/detail responses immutably with checksums. Accept only exact symbol matches; mark repeated,
ambiguous, renamed, migrated, or otherwise non-unique identity evidence unresolved for review.

Decision: scope admission requires explicit per-contract crypto, stablecoin, and leveraged-token
classification evidence. Archive-only candidates remain quarantined when any required classification
is unknown. The legacy static stablecoin list is not admission evidence, and ticker suffixes remain
prohibited as leveraged-token evidence. Eligibility requires an exact official trading start, applies
the fixed 30-day age rule, and stops strictly at an exact known announcement publication time; current
status never back-filters history.

Reason: archive data proves observation, while current metadata and announcement evidence answer
different questions. Keeping these layers separate prevents survivorship and look-ahead leakage.
This lifecycle work occurred before scanner evaluation and did not inspect any score outcomes.

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

## 2026-08-23 — Milestone review remediation cycle 2 (final allowed cycle)

Decision: treat missing, empty, null, malformed, or blank classification evidence as unresolved;
require a nonempty official underlying-subtype list and nonblank provenance; and quarantine rather
than infer. Remove the raw-archive overwrite parameter and verify bytes before exclusive creation.
Introduce structured acquisition failures that distinguish archive absence, checksum-sidecar
absence, checksum parsing, payload retrieval, verification mismatch, and immutable-path conflicts,
while preserving every available hash and failing full processing on verification defects.

Reason: remediation review 1 found the prior fix partial for malformed classification inputs and
failure-path manifests, plus a direct raw overwrite bypass. These are implementation/auditability
corrections made without inspecting scanner outcomes, validation, or holdout data.

## 2026-08-23 — One-time additional acquisition-boundary remediation authorization

Decision: accept the user's narrow authorization for one additional implementation-integrity
attempt after the two prior remediation cycles. The attempt is limited to canonical identity
validation, strict archive-key and raw-root confinement, processing-time checksum verification,
verified no-replace raw installation, and collision-safe exclusive manifest creation.

Reason: the final independent gate at `2f9d8ab` identified five remaining acquisition-boundary
blockers. Scanner features, HotScore, structure and outcome definitions, episodes, baselines,
chronological splits, and advancement criteria remain frozen. No validation/holdout inspection,
performance analysis, or full-history acquisition is authorized.
