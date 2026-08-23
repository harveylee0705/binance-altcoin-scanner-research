# Lifecycle Resolution Independent Review — 2026-08-24

- Reviewed implementation range: `be59d7899edaf19afafffea9991e9f215fa8c71b` through
  `ef7de43902ded35921ba6bda14855720c463c667`
- Lifecycle bundle ID: `ce4cdbe61ae51ceb01b000710687d949b635ba47a9c30c4adbefe7c94d709a35`
- Independent reviewer: milestone-reviewer (read-only)
- Verdict: **FAIL**
- Findings: 0 CRITICAL, 2 HIGH, 1 MEDIUM
- Automatic remediation performed after review: no

## Blocking findings

### HIGH — Scope registry completion is automatic, not an auditable review gate

The builder creates a fresh complete registry on each run. A newly discovered current
`COIN`/`USDT` perpetual that is absent from the static stablecoin positives can receive automatic
stablecoin and leveraged-token negatives and be admitted. Digest comparison does not protect the
authorization path because the generated registry replaces the previous registry instead of
requiring a new audit. The three current positive stablecoin records also do not retain evidence
that directly establishes stablecoin status.

Required remediation identified by the reviewer: accept a separately reviewed registry keyed to
the candidate digest; keep new-digest records unresolved until review; retain direct positive
exclusion evidence; and rebuild exchange-info classifications from a bound raw snapshot.

### HIGH — Bundle verification does not reconstruct most primitive evidence

Trade ZIPs are rehashed and reparsed, but final authorization does not independently rehash and
reparse the referenced archive XML, CMS catalog/details, or raw exchange-info snapshot. It compares
several mutually consistent derived artifacts rather than reconstructing candidate discovery,
scope, announcements, archive keys, and readiness from all primitive evidence. A builder defect
could therefore create a self-consistent but incorrectly derived bundle that passes verification.

Required remediation identified by the reviewer: content-bind the raw exchange-info,
archive-index/checkpoint, CMS catalog/detail manifests, and provenance, then reconstruct and compare
all derived lifecycle decisions during bundle verification.

### MEDIUM — Executable downloads do not recheck the approved code commit

The approved-commit check occurs during plan preparation but is not repeated by the executable
downloader. Code or configuration drift after plan creation can therefore escape the intended
commit pin if the plan's bound artifacts remain unchanged. Approval/review pins also have no
explicit supersession mechanism.

Required remediation identified by the reviewer: recheck the runtime commit immediately before
dry-run validation and execution, and define approval revocation/supersession semantics.

## Reviewer conclusions

- The live-boundary logic is defensible for catalog-derived eligibility, and `KNCUSDT` fails closed
  on conflicting age evidence.
- The 30-day rule is enforced from `eligibility_age_anchor_at`.
- Exact official starts and observed first-trade evidence are kept distinct.
- The delisting acquisition inspected all 426 catalog-161 articles without a literal-title filter.
- Missing exact delisting cutoffs remain null; the 70 completed-search/no-reliable-timestamp cases
  are not approximated.
- The ten blocked contracts are conservative conflicts, not lowered evidence quality.
- The threat model is appropriately narrow and does not demand unnecessary PKI or malicious-operator
  resistance.
- The bundle is not ready for an independent authorization audit because the registry gate and
  primitive-evidence reconstruction remain incomplete.

## Checks actually run by the reviewer

- `pytest`: 198 passed
- Ruff: passed
- `git diff --check be59d78..ef7de43`: passed
- Worktree: clean during review
- Bundle ID and candidate digest: matched the supplied values
- Unicode contracts: confirmed cataloged, trade-probed, stored through hashed paths, and derived-ready
- Independent raw-trade verification: started and stopped after several minutes without error when
  the approximately 10 GB / 672-archive reparse did not complete within the bounded review

The reviewer did not run the writing development slice, live reacquisition, scanner performance,
validation, final holdout, or full-history OHLCV. No remediation-review cycle was started, in
accordance with the user's instruction to stop after recording the independent review.

## Recommendation

`LIFECYCLE GATE STILL INCOMPLETE`
