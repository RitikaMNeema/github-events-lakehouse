# Quality gate report

Run 2026-11-25 17:01 UTC over `2023-06-15T00` .. `2023-06-15T00` UTC.

| Severity | Check | Detail |
|---|---|---|
| PASS | manifest completeness | all 1 hour(s) accounted for |
| PASS | volume anomaly | all hours within tolerance of median 145,538 |
| PASS | event_id uniqueness | no duplicates |
| PASS | quarantine rate | 0 quarantined of 145,538 (0.000%) |
| PASS | dimension keys | no null dimension keys |
| PASS | schema drift | every top-level source key is persisted; bronze is lossless |
| PASS | freshness | last processed 0.0h ago |

**Result: PASSED**
