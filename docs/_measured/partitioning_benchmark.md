# Partition layout benchmark

Measured 2026-11-25 19:50 UTC on 1,669,055 rows, target file size 64 MB, median of 3 runs after a discarded warm-up.

## Layouts compared

| Layout | Partitioning | Data files | Files after date prune | Avg file size |
|---|---|---|---|---|
| `unpartitioned` | none | 3 | 3 | 12.35 MB |
| `day_partitioned` | PARTITIONED BY (days(event_ts)) | 2 | 1 | 18.84 MB |
| `day_sorted` | PARTITIONED BY (days(event_ts)) + sorted by repo_id | 2 | 1 | 18.85 MB |

## Query wall time

| Query | unpartitioned | day_partitioned | day_sorted |
|---|---|---|---|
| single-day scan | 0.066s | 0.062s | 0.048s |
| day + group by type | 0.099s | 0.085s | 0.078s |
| full-table aggregate | 0.100s | 0.073s | 0.082s |

> The slice spans **2 days**, so the single-day predicate genuinely prunes partitions and the files-pruned column below is meaningful.


## Reading these numbers

* **Files after date prune** is the structural result. The unpartitioned table must open every file to answer a single-day question; the partitioned tables open only the day's files. That ratio holds at any scale.
* **Wall time** is single-node and cache-warm. The direction is meaningful, the magnitude is not: at this data volume a full scan is cheap, so pruning looks less dramatic than it is on a real cluster.
* **`day_sorted`** costs more to write (an extra sort-rewrite pass) and only pays off on queries that filter by `repo_id`. It is the right default only if that is the access pattern - which is why the production tables declare it explicitly rather than inheriting it.
* **Full-table aggregate** should be roughly equal across all three. If partitioning appears to speed up a query that reads everything, the measurement is wrong.
