# Primitive evidence replay verification summary

- Verdict: PASS
- Implementation commit: `e561aa8c2be5f7b634016e327a818e2fcd74078b`
- Lifecycle bundle ID: `f1296cd9a868a79bb4d1bb3a28606117a31ab42df22d4f535ea12cbe57720813`
- Lifecycle bundle SHA-256: `f9a8391c8dac8441d6b598111d5166821679055f17a0ee234c252ceca3c6eb45`
- Primitive manifest ID: `9fc12a931b60738bc326a612411bb4b355cf7c70e5cf3d30ecd886cb1856121b`
- Primitive manifest SHA-256: `af0f95d603429e3f9f3f7cecfebef73f35ae9bdae5aaffc3a4292bd79bc377c3`
- Verification report ID: `13522e7fbc15423ce9a5bbd06f8c9e5359de278709a5de277a8f9630113f46bf`
- Verification report SHA-256: `898a02a8f8a8170f89e73ee2f3c2fde2c8af0f8ce8b772704ef0a845bdc895fa`
- Candidate identities replayed from raw archive XML: 986
- Derived catalog rows reproduced exactly: 935
- Archive symbol rows replayed: 935
- CMS evidence rows replayed: 1,002
- First-trade ZIPs rehashed and reparsed: 672
- Daily boundary indexes replayed: 12
- Authorization readiness reproduced: true

The full review-time verifier rehashed every bound primitive, reconstructed candidates and monthly
ZIP keys from raw XML, strictly reparsed exchangeInfo and CMS JSON, verified the reviewed registry
and scope-review artifact, reparsed the local first-trade archives, checked daily boundary indexes,
and reproduced the lifecycle catalog exactly. Runtime validation consumes this content-bound report
and manifest rather than repeating the full replay for every future OHLCV object.

Raw report directory identifier:
`20260823T132656988386Z_27564_31db394b558bd48d_catalog_20260824T010937256427Z_30140_c9008b21c517a0cb`.
