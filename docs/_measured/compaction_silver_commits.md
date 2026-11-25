# Compaction: `silver.commits`

Measured 2026-11-25 06:23 UTC, target file size 128 MB.

| Metric | Before | After |
|---|---|---|
| Data files | 6 | 1 |
| Avg file size | 9.1 MB | 56.6 MB |
| Smallest file | 8.3 MB | 56.6 MB |
| Largest file | 10.1 MB | 56.6 MB |
| Total size | 54.6 MB | 56.6 MB |
| Records | 761,173 | 761,173 |

Record count must be identical before and after. Compaction rewrites file layout only; if this number moves, the job has a bug.
