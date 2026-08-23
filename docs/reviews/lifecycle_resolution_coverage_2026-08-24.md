# Sanitized Lifecycle Resolution Coverage — 2026-08-24

This is a metadata-only audit summary. It contains no HotScore/outcome relationships, scanner
performance, returns, barrier results, validation observations, final-holdout observations, raw
market data, or secrets.

- Starting commit: `6a1bf93cdb3055866fbb91caff6c20e84d56fad7`
- Reviewed implementation commit: `ef7de43902ded35921ba6bda14855720c463c667`
- Lifecycle bundle ID: `ce4cdbe61ae51ceb01b000710687d949b635ba47a9c30c4adbefe7c94d709a35`
- Candidate registry digest: `b323b3c3ba1010654d9ce1a47325364f7c7e624624857554b7e945f38f91c445`
- Authorization ready: no
- Real approval record created: no
- Independent review verdict: **FAIL**

## Discovery and scope

- Archive identities: 986
- Canonical archive symbols: 932
- USDT candidates: 832 (829 canonical and 3 Unicode/noncanonical)
- In-scope crypto perpetual identities, including the BTC benchmark: 672
- Scope-unresolved identities: 0
- Stablecoin positives: 3 (`FRAXUSDT`, `USDCUSDT`, and `USTCUSDT`)
- Leveraged-token positives: 0
- Stablecoin finite-universe negative records: 829
- Leveraged-token finite-universe negative records: 832

Scope dispositions across all 986 archive identities:

- `in_scope_crypto_perpetual`: 671
- `benchmark_only`: 1
- `excluded_noncrypto_or_index`: 152
- `excluded_non_usdt_product`: 103
- `excluded_delivery_or_settlement_archive_identity`: 51
- `excluded_composite_or_index`: 3
- `excluded_stablecoin`: 3
- `excluded_noncrypto_backed_or_non_altcoin`: 2

The independent review found that these records are candidate-digest-bound but are generated
automatically rather than supplied as a separately reviewed registry. Consequently, the zero
unresolved count must not be interpreted as an independently defensible scope-completion gate.

## Lifecycle evidence for the 672 in-scope identities

Age-anchor basis:

- Exact official original launch: 58
- First observed Binance Futures trade: 611
- Legacy pre-research adjudication: 2
- Unresolved: 1

Delisting status:

- Currently trading / not applicable: 513
- Exact publication cutoff: 80
- Completed search with no reliable exact timestamp: 70
- Conflicting evidence requiring adjudication: 9
- Incomplete search: 0

Readiness:

- Ready under the derived catalog rules: 662
- Blocked: 10
- `KNCUSDT`: first observed trade precedes the claimed exact launch
- Delisting/relisting conflicts: `AERGOUSDT`, `AIAUSDT`, `CTKUSDT`, `CVCUSDT`, `CVXUSDT`,
  `MAVIAUSDT`, `OMGUSDT`, `SLPUSDT`, and `XEMUSDT`

All ten remain fail-closed. Four onboard/start discrepancies are retained as warnings rather than
silently rewritten.

## Unicode identities

The three Unicode USDT contracts are preserved by semantic identity, stored through hashed safe
path components, first-trade-probed, and marked ready by the derived catalog:

- `币安人生USDT` → `identity_c5309282860157198c75103ede8ba6abf0b53a29562f9d02cd66c390ab2b5132`
- `我踏马来了USDT` → `identity_31756917f4ce15e128f5077850851a864bd050107cdfd1333cd01cefc76d9a50`
- `龙虾USDT` → `identity_f480f3b9900b6146e6e774ca46bb4186f2d8f1f3134ba82c09f992de9d3283ad`

## Verification boundary

- Implementation checks: 198 tests passed; Ruff passed; `git diff --check` passed.
- The implementation-side lifecycle bundle verifier completed over 672 first-day trade archives
  (approximately 10 GB) and reported 662 ready identities and the ten blockers above.
- A development-only 20-archive vertical slice completed for engineering continuity without
  inspecting or reporting scanner performance.
- The independent reviewer independently passed the 198 tests, Ruff, and diff check. Its separate
  full raw-trade reparse was stopped after several minutes without error because it did not finish
  within the bounded review.
- Validation, final holdout, scanner performance, and full-history OHLCV were not run or inspected.

Full-history acquisition remains prohibited.
