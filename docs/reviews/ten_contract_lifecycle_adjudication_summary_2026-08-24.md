# Ten-contract lifecycle adjudication summary

Adjudication ID: `d6ce1c7575cdd243eb16690da84afb5dc2829f5a373f92de0e797f561073a831`

The ten requested conflicts were resolved as follows:

| Contract | Disposition |
|---|---|
| `KNCUSDT` | The scheduled launch was six minutes after checksum-verified trading; the scheduled time is demoted and the verified first trade remains the conservative anchor. |
| `AERGOUSDT` | Genuine termination and relisting; two episodes, with the spot-delisting match rejected as another product. |
| `AIAUSDT` | Genuine termination and 2026-01-20 relisting; the unrealized/superseded 2026-01-16 launch claim is rejected. |
| `CTKUSDT` | Genuine termination and relisting; two episodes. |
| `CVCUSDT` | Genuine termination and relisting; spot/margin matches are rejected as other products. |
| `CVXUSDT` | Genuine termination and relisting; two episodes. |
| `MAVIAUSDT` | Genuine termination and relisting; two episodes. |
| `OMGUSDT` | Terminal episode with two official postponements incorporated; no relisting. |
| `SLPUSDT` | Genuine termination and relisting; two episodes. |
| `XEMUSDT` | Terminal Futures episode; the earlier spot article is rejected as another product. |

The stricter production parse also surfaced three evidence-backed relistings and they were resolved
fail-closed rather than suppressed: `ICPUSDT`, `BNXUSDT`, and `TLMUSDT`. `BNXUSDT` has a terminal
second episode; its final delisting-publication timestamp remains null because the complete official
CMS search found no reliable exact publication record.

All relisting intervals block pre-live timestamps and terminated gaps, apply each episode's own
announcement cutoff, and reset the fixed 30-calendar-day age clock from the new live boundary.
