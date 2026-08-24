# Independent eligibility-oracle summary

- Final implementation commit: `47b5feb45aec82f967ff5679fb8166f89039356a`
- Oracle report schema: `independent-eligibility-oracle-report-v1`
- Verification report ID: `a5d3cbc37092e73898a7ff4088bd00c947741f01472d5ea50b29e6d96c9b5c64`
- Verification report SHA-256: `2ef4152d626d562dd2801cda1cb131b13485557d8613abb0741366f8aa5b5653`
- Primitive manifest ID: `7b48cc65ffadb1abb6575569366262af33adf052825de74246fb952817a7bff9`
- Primitive manifest SHA-256: `38e550581f3264d7fac02dcaefb3f74ebd722f4fc6bcfc54104cec8f256b81e6`
- Episode-boundary evidence ID: `b10265abbc50a4174899226478772fcf61db4e2b1df2d95b1200f3d21ea23a0f`
- Episode-boundary evidence SHA-256: `ceefc14c1f52de7070b53388f7d74adc943f8e7128673f513d60e5598bc2d591`
- Production catalog SHA-256: `4b5abbce652fd5e951fa69676360aea37433007f126ba89307b0c6fd94c7d29b`
- Candidate count: 986
- Independently derived in-scope contracts: 672
- Independently derived lifecycle episodes: 682
- Independently derived ready contracts: 672
- Independently derived blockers: 0
- Oracle verification status: `PASS`
- Final status: `PASS`

The approval-time oracle independently reads and validates content-bound primitive/reviewed inputs,
derives episode anchors, 30-day boundaries, gaps, terminal boundaries, delisting cutoffs, conflicts,
and readiness, and only then compares those values with the production catalog. Its core logic does
not import or call the production announcement parser or production lifecycle catalog builder.

Mutation coverage rejects early anchors, exact-listing leakage, missing relist age resets, continuous
eligibility across gaps, wrong or missing runtime-effective ends, wrong article/time/symbol/episode
cutoffs, missing and fabricated cutoffs, false readiness, wrong episode trade evidence roles, wrong
symbol/archive/date/member/checksum/manifest lineage, and stale or mismatched PASS reports.

