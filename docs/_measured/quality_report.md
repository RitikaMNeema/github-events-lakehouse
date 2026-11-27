# Quality gate report

Run 2026-11-26 08:40 UTC over `2023-06-15T00` .. `2023-06-15T23` UTC.

| Severity | Check | Detail |
|---|---|---|
| PASS | manifest completeness | all 24 hour(s) accounted for |
| PASS | volume anomaly | all hours within tolerance of median 165,772 |
| PASS | event_id uniqueness | no duplicates |
| PASS | quarantine rate | 0 quarantined of 3,828,108 (0.000%) |
| PASS | dimension keys | no null dimension keys |
| PASS | schema drift | every top-level source key is persisted; bronze is lossless |
| PASS | freshness | last processed 0.0h ago |

**Result: PASSED**
