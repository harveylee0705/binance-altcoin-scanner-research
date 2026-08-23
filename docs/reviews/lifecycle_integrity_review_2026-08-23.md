# Lifecycle / Integrity Independent Review — 2026-08-23

Reviewed implementation commit: `ed0731e2467116d7f147a6d6a1729f3f71dcb3da`

Starting commit: `dd4c5338eb46a276005b673413f329f7c4d708cc`

Independent verdict: **FAIL**

This review covered lifecycle acquisition, announcement semantics, classification evidence,
provenance, numeric integrity, archive planning, and full-run authorization. It did not inspect
scanner performance, validation observations, or final-holdout observations. Full-history OHLCV
acquisition was not executed.

## Blocking findings

1. **HIGH — Announcement action/time binding remains insufficient.** Mixed-purpose body clauses
   and table-context bleed can still promote product-enablement or unrelated operational times into
   exact listing starts. The parser needs positive grammatical/structural binding between the exact
   perpetual-contract action, symbol, and timestamp, plus regressions for body-only enablement and
   context termination.
2. **HIGH — Delisting search completion is overstated.** Candidate acquisition requires the word
   `delist`, although applicable official titles may instead say settle, cease trading, or close all
   positions. Archive-only `searched_official_sources_no_exact_announcement_found` therefore does
   not yet prove an exhaustive applicable search.
3. **HIGH — Authorization readiness is not independently rederived from primitive evidence.** The
   verifier trusts derived catalog readiness fields, does not enforce a canonical approved/current
   bundle or code commit, and can accept a self-consistent forged or stale ready bundle. It needs
   primitive invariant recomputation and stale/superseded-bundle controls within the stated local
   integrity threat model.
4. **MEDIUM — Resume provenance is incomplete.** A resumed archive checkpoint can be trusted without
   rehashing/reconstructing every referenced raw XML page, and legacy archive acquisition fields are
   not modeled as explicitly unresolved as rigorously as announcement and exchange-info timestamps.

No CRITICAL findings were reported.

## Metadata-only coverage at review

- Bundle ID: `5ea0d3a151d7ba4708045c3254fe4397130ce78c2de64c82ebe5be69b294a624`
- Archive prefixes discovered: 986
- Canonical catalog USDT rows: 829
- Additional noncanonical USDT blockers: 3
- Total reported USDT candidates: 832
- Exact USDT original trading starts: 60
- Exact delisting-publication cutoffs: 63
- Stablecoin classification: 1 true, 0 false, 828 unresolved
- Leveraged-token classification: 0 true, 0 false, 829 unresolved
- Current `TRADING`: 672
- Current non-trading / `SETTLING`: 126; 69 lack an exact applicable delisting announcement
- Archive-only canonical USDT: 31; 27 marked searched/no exact announcement, subject to the search
  completeness blocker above
- Historical-inclusion ready: 0
- Quarantined/unresolved reported candidates: 832
- Material onboard/start discrepancies: 4
- Current-status conflicts requiring adjudication: AIAUSDT and MAVIAUSDT are currently `TRADING`
  while matched delisting-publication evidence is present
- `USTCUSDT`: stablecoin true from the frozen positive guard; leveraged status unresolved; blocked
- `BTCUSDT` and `ETHUSDT`: listing start unresolved; blocked

## Announcement re-audit

- Former accepted starts: 535
- Former starts rejected under corrected evidence semantics: 475
- Currently accepted exact original starts across all catalog quotes: 61
- Ambiguous or unresolved listing starts across all catalog rows: 871
- Former Copy Trading false positives identified: 98 symbols from 9 articles
- Former pre-market/product-enablement false positives identified: 28 symbols from 28 articles

## Verification evidence

- `pytest`: 183 passed
- `ruff check .`: passed
- `git diff --check`: passed
- Development-only engineering slice: completed by the implementation agent using 20 bounded monthly
  archives and the baseline catalog for engineering continuity; no performance summary was produced
- Independent reviewer did not rerun the slice because the reviewer was read-only
- Validation/final holdout inspected: no
- Full-history OHLCV run executed: no
- Current-month daily/API tail: not implemented; remains a pre-evaluation acquisition blocker

## Recommendation

`DO NOT RUN FULL HISTORY — LIFECYCLE/INTEGRITY GATE STILL INCOMPLETE`
