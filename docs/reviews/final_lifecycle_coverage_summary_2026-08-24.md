# Final lifecycle coverage summary

- Final implementation commit: `47b5feb45aec82f967ff5679fb8166f89039356a`
- Lifecycle catalog schema: `binance-usdm-lifecycle-v5`
- Lifecycle bundle ID: `c5a0b96843e80879889f05569f47ecfd007d3cbe95b1f5e85255f185d6fb4813`
- Candidate digest: `b323b3c3ba1010654d9ce1a47325364f7c7e624624857554b7e945f38f91c445`
- Candidate count: 986
- USDT candidates: 832 (829 canonical and 3 noncanonical)
- Reviewed in-scope/benchmark contracts: 672
- Stablecoin exclusions: 3
- Leveraged-token exclusions: 0
- Normal one-episode contracts/episodes: 662 / 662
- Relisted contracts/episode records: 10 / 20
- Total lifecycle episodes: 682
- Checksum-verified first-trade age anchors: 682 / 682 episodes
- Unresolved age anchors: 0
- Reviewed exact delisting cutoffs: 81
- Reviewed with no reliable exact cutoff: 82
- Current episodes where a cutoff is not applicable: 519
- Unresolved delisting decisions: 0
- In-scope contracts ready: 672 / 672
- Lifecycle blockers: 0
- Authorization readiness: true
- Descriptive exact official trading-start coverage: 524
- Top-level exact official delisting-publication coverage: 73
- Top-level exact official last-trading coverage: 45
- Lifecycle catalog SHA-256: `4b5abbce652fd5e951fa69676360aea37433007f126ba89307b0c6fd94c7d29b`
- Coverage report SHA-256: `2e57921cefa66a79a4820292a3f424d46b58331b1a9d8c21f66f179d9b494230`
- Eligibility-oracle report ID: `a5d3cbc37092e73898a7ff4088bd00c947741f01472d5ea50b29e6d96c9b5c64`

`BTCUSDT` and `ETHUSDT` are separately represented as benchmark contracts. Each has one
checksum-verified first-trade age anchor, is ready, and has no accepted exact delisting cutoff.

Genuine relisting lifecycles were established for `AERGOUSDT`, `AIAUSDT`, `BNXUSDT`,
`CTKUSDT`, `CVCUSDT`, `CVXUSDT`, `ICPUSDT`, `MAVIAUSDT`, `SLPUSDT`, and `TLMUSDT`.
Each of their two episodes has an independently bound first-trade anchor, including a post-gap
anchor for episode 2.

No real full-history approval was created, no full historical OHLCV was executed, and no scanner
outcomes, validation, or final holdout results were inspected.
