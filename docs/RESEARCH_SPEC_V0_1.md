# Research Specification — Scanner v0.1

Status: **frozen before outcome inspection**

Internal timezone: **UTC**

Score scale: **0.0–1.0**

## Objective and sequence

The sole v0.1 question is whether an equal-weight Hot Pick Scanner identifies emerging Binance
altcoin leaders that subsequently outperform. This phase is selection research, not trading or PnL
simulation. Work must remain sequential: (1) scanner selection edge, (2) Volume Profile/setup
location, (3) entries, (4) risk sizing, (5) risk-neutral pyramiding, and (6) exits/trend capture.

No later-phase feature may be introduced into this milestone. Prohibited items include Volume
Profile/VAH/VAL/POC, entry timing, stops, leverage, margin trading, pyramiding, sizing, take-profit,
trailing exits, funding-aware PnL, portfolios, optimizers, ML, social/news data, funding or OI in
HotScore, and common technical-signal additions.

## Anti-overfitting protocol

- Do not optimize historical PnL or search large parameter grids.
- Prefer broad monotonic relationships or plateaus to narrow peaks.
- Development, validation, and final holdout are chronological and immutable.
- Validation is not a second training set. Final holdout remains untouched until v0.1 is frozen.
- Every post-result scanner change becomes a documented version recording the change, rationale,
  hypothesis, and dataset authorized to evaluate it.
- At timestamp `t`, features use only data available at or before `t`.
- Explicitly audit look-ahead, survivorship, listing, and future-liquidity leakage.

## Point-in-time market universe

Scope is Binance USD-M, USDT-quoted and USDT-margined **perpetual futures**. Include every contract
that existed at the historical timestamp, including later-delisted contracts. Exclude BTCUSDT from
tradable rows but retain its data as benchmark. Include ETHUSDT with `is_eth=true`. Exclude stablecoin
underlyings, leveraged tokens, COIN-M, delivery/quarterly, USDC, and non-crypto/TradFi perpetuals.
Spot and margin are out of scope.

Scope classification is a mandatory pipeline invariant, not a caller convention. It requires
explicit quote asset, margin asset, contract type, underlying type, and leveraged-token evidence.
Unresolved classification fails closed. Token-name suffixes such as `UP` are not classification
evidence because legitimate assets (for example JUP and SYRUP) share those characters.

Target coverage is 2020-01-01 through the latest fully completed available data; a contract begins
only when it existed. Eligibility starts at the later of the official futures listing timestamp and
30 full calendar days after that timestamp, and requires valid market data. Where an official
delisting announcement timestamp is known, new scanner events stop at that time. No announcement
timestamp may be invented. Preserve listing/onboard time, first valid data time, announcement time,
last valid/trading time, delisting/delivery time, and latest observed status with provenance.

There is no hard liquidity threshold. Store rolling 24H quote volume, rolling 30D median of daily
quote-volume observations, and the timestamp-level cross-sectional liquidity percentile.

## Raw data and 4H convention

Canonical input is immutable Binance USD-M 1H OHLCV: open time, open, high, low, close, base volume,
quote volume, close time, and trade count (plus source fields when present). Store BTC identically.
Funding is collected when practical but never enters HotScore v0.1.

Scanner bars open at **00:00, 04:00, 08:00, 12:00, 16:00, and 20:00 UTC**. A completed 4H bar must
contain exactly four unique, consecutive hourly bars at offsets 0, 1, 2, and 3 hours. Aggregate open
first, high maximum, low minimum, close last, volumes/trades sum, and close time last. Duplicate,
misaligned, missing, or incomplete groups are rejected and logged; OHLCV is never interpolated or
forward-filled. A scanner timestamp is the 4H close time, while `open_time` remains the bar key.
All symbol-local rolling windows and HOT episodes restart after any missing 4H timestamp; no feature
or continuous episode may bridge a data gap.

## Frozen features

For each eligible tradable contract after a completed 4H bar:

1. Close-to-close returns use exactly 6, 18, and 42 bars (1D/3D/7D), ranked cross-sectionally with
   average-tie percentiles as `rs_1d_pct`, `rs_3d_pct`, and `rs_7d_pct`.
2. `vol_exp_4h_raw` is current quote volume divided by the median of the **previous** 30 completed
   4H quote-volume bars. Current volume is excluded. Rank to `vol_exp_4h_pct`.
3. `current_24h_quote_volume` sums the last 6 completed 4H bars including current. Its baseline is
   the median of the previous 20 completed rolling-24H observations, excluding current. The ratio is
   `vol_exp_24h_raw`, ranked to `vol_exp_24h_pct`. Overlap is retained by design.
4. HotScore is the equal-weight arithmetic mean of those five percentiles. `hot_score_pct` is the
   cross-sectional percentile of HotScore. `is_hot` means `hot_score_pct >= 0.9`; this is not an
   optimized cutoff. Primary analysis uses D1–D10 across the full score distribution.

Rows missing any component do not receive HotScore and cannot be HOT.

## Price structure tags (not score inputs)

`prev_20d_high` is the maximum high over the previous 120 completed 4H bars, excluding current.
True range is the maximum of high-low, abs(high-previous close), and abs(low-previous close).
ATR(14) is Wilder-style: seed with the mean of the first 14 true ranges, then recursively update
`ATR_t = ((13 × ATR_(t-1)) + TR_t) / 14`.

`distance_to_20d_high_atr = (prev_20d_high - close) / atr_14`. Stage precedence is deterministic:

1. `TRIGGERED`: current close exceeds current previous-20D-high and prior close did not exceed the
   prior row's corresponding previous-high.
2. `EARLY_TREND`: most recent trigger was 1–3 completed bars ago and close is at most 2 current ATR
   above that trigger's breakout level.
3. `EXTENDED`: after any known trigger, close is more than 2 current ATR above its breakout level.
4. `APPROACHING`: not above the high and distance is 0–0.75 ATR inclusive.
5. `OTHER`.

The relevant breakout level is frozen as `prev_20d_high` on the trigger bar and forward-filled only
within that symbol. A retracement after an old breakout is `OTHER` unless another rule applies.

## Observations and independent episodes

The observation dataset contains every eligible coin × scanner timestamp. An independent HOT
episode begins on a transition from non-HOT (or no prior eligible observation) to HOT and ends on
leaving HOT. Continuous HOT rows share one deterministic episode ID; later re-entry creates another.
No cooldown is present in v0.1.

## Future labels

Future data labels outcomes and is never a feature. Close returns over 6/18/42 future bars produce
1D/3D/7D returns. BTC-relative return is alt return minus BTCUSDT return for the same signal timestamp
and horizon. Cross-sectional excess is alt return minus the median eligible altcoin return for the
same signal timestamp and horizon. The primary horizon is 3D; 1D and 7D are secondary.

For 3D and 7D, future windows contain bars 1 through N after the signal (the signal bar is excluded):

- `MFE = max(future high / signal close - 1)`
- `MAE = min(future low / signal close - 1)`

For +2/-1 ATR, signal close and signal ATR are frozen. Scan future completed bars in time order.
Upper first is success; lower first is failure; neither is censored. If both barriers occur in the
same 4H bar, OHLC cannot identify order, so the event is labeled `ambiguous` rather than forced.
Compute both 3D and 7D for episode starts and an unconditional eligible-observation baseline.

## Baselines and diagnostics

Compare combined HotScore with: (A) 3D relative-strength percentile, (B) 24H volume-expansion
percentile, (C) fresh previous-20D-high breakout, and (D) random/unconditional eligible universe.
Diagnostics may stratify BTC regime, breadth, volatility, calendar period, ETH inclusion, liquidity,
contract age, and active-versus-delisted status, but none conditions the primary v0.1 scanner.

## Frozen splits and access rules

- Development: 2020-01-01 through 2023-06-30.
- Validation: 2023-07-01 through 2024-12-31, used once v0.1 is frozen to test persistence only.
- Final holdout: 2025-01-01 through latest complete data, untouched during initial development.

Default code access is development-only. Forward labels must themselves fit inside the authorized
split; boundary rows without a complete future window are null/censored, never borrowed across a
split boundary.

## Pre-registered advancement criteria

Advance to a Volume Profile experiment only if unseen evaluation shows: positive D10 3D and 7D
cross-sectional excess; HOT episode +2/-1 success probability materially above unconditional (target
about 25% relative improvement); no domination by one/a few symbols, ETH, or one bull interval; and a
broadly ordered/plateau score relationship rather than a magic bucket. Failure is not reinterpreted.
