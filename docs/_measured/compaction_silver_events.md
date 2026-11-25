# Compaction: `silver.events`

Measured 2026-11-25 06:22 UTC, target file size 128 MB.

| Metric | Before | After |
|---|---|---|
| Data files | 6 | 1 |
| Avg file size | 3.8 MB | 25.7 MB |
| Smallest file | 3.4 MB | 25.7 MB |
| Largest file | 4.2 MB | 25.7 MB |
| Total size | 22.8 MB | 25.7 MB |
| Records | 848,098 | 848,098 |

Record count must be identical before and after. Compaction rewrites file layout only; if this number moves, the job has a bug.
