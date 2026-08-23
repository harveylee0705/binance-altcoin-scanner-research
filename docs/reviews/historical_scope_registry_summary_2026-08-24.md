# Historical Scope Registry Summary — 2026-08-24

This sanitized summary describes the generated registry bound to lifecycle bundle
`ce4cdbe61ae51ceb01b000710687d949b635ba47a9c30c4adbefe7c94d709a35`. It is not an approval
record and does not authorize full-history acquisition.

## Identity and finite universe

- Reviewed implementation commit: `ef7de43902ded35921ba6bda14855720c463c667`
- Candidate registry digest: `b323b3c3ba1010654d9ce1a47325364f7c7e624624857554b7e945f38f91c445`
- Exact archive-identity universe: 986
- USDT candidates: 832
- Stablecoin positives: 3
- Stablecoin finite-universe negatives: 829
- Leveraged-token positives: 0
- Leveraged-token finite-universe negatives: 832
- Derived in-scope set, including the BTC benchmark: 672
- Derived unresolved set: 0

Every candidate has an explicit product disposition. Stablecoin and leveraged-token negatives are
recorded over the finite candidate universe rather than inferred by absence at downstream use.
Product identities keep canonical semantic symbols, including Unicode, while unsafe filesystem
names use deterministic SHA-256-based components.

## Independent-review qualification

The registry is reproducible and digest-bound, but the independent reviewer found that it is not a
genuine re-audit gate:

1. The builder replaces the registry on each run and automatically assigns negatives to newly
   discovered current `COIN`/`USDT` perpetuals that are not on the configured positive list.
2. A changed candidate digest therefore does not leave new records unresolved pending a separate
   review.
3. The positive stablecoin records cite product/underlying metadata that does not itself establish
   stablecoin status.
4. Current exchange-info classifications are not reconstructed from a content-bound raw snapshot
   during final authorization verification.

Accordingly, this registry is useful as a deterministic generated inventory, but its completeness
and exclusion evidence are not yet sufficient for authorization. No real approval record was
created.
