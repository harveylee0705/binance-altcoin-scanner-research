# Reviewed historical scope registry summary

- Candidate-set digest: `b323b3c3ba1010654d9ce1a47325364f7c7e624624857554b7e945f38f91c445`
- Candidate identities reviewed: 986 (832 USDT candidates; 672 in-scope crypto perpetual/benchmark candidates)
- Registry payload ID: `fd5682792ec1a7a863e5f0b524f8197728a0c3c81f08cb0b90e2ee6130e6d775`
- Reviewed registry ID: `4ca54688743f5e53bcbad642ebeedc7f5f821d273983bedd8805befc79b62908`
- Registry file SHA-256: `b7a06d6735eb57a9d1a3cfa56f73bb13ee4ff7eb54a79dce822144c72884a33e`
- Independent scope-review ID: `b230224b7b09a55dba01f23fdde9df15e3586d744eb702597bcafa299d2e91d3`
- Independent review verdict: PASS
- Stablecoin positive exclusions: `FRAXUSDT`, `USDCUSDT`, `USTCUSDT`
- Leveraged-token positive exclusions: none in this exact finite candidate set
- Noncrypto/index/composite exclusions: 158
- Unresolved scope candidates: 0
- Unicode identities reviewed: `币安人生USDT`, `我踏马来了USDT`, `龙虾USDT`

The registry is a separately maintained input. Normal lifecycle builds may generate a candidate
inventory, but may not generate or replace reviewed dispositions. A changed candidate digest stops
the build and emits a review-required difference without assigning negatives.

Direct positive stablecoin evidence is recorded in the registry from Frax documentation, Circle
USDC documentation, and Binance Research for USTC. Candidate-bound finite-universe negatives are
used for all reviewed non-positive classifications.
