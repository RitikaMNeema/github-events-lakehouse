# Compaction: `silver.events`

Measured 2026-11-26 07:02 UTC, target file size 128 MB.

| Metric | Before | After |
|---|---|---|
| Data files | 7 | 2 |
| Avg file size | 7.0 MB | 25.5 MB |
| Smallest file | 3.3 MB | 24.3 MB |
| Largest file | 26.7 MB | 26.7 MB |
| Total size | 48.7 MB | 51.0 MB |
| Records | 1,669,055 | 1,669,055 |

Record count must be identical before and after. Compaction rewrites file layout only; if this number moves, the job has a bug.
