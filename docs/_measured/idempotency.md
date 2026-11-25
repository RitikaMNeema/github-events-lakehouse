# Idempotency evidence

Range `2023-06-15T00` .. `2023-06-15T05` UTC, measured 2026-11-25 21:53 UTC.

## Method

`bronze_raw_to_iceberg.py` was run twice over the same range with no intervening change. Bronze is partitioned by `(ingest_date, ingest_hour)` and written with Iceberg's `overwritePartitions()`, so the second pass replaces exactly the partitions it rewrites.

## Data must be identical

| Measure | Pass 1 | Pass 2 | Verdict |
|---|---|---|---|
| rows | `848106` | `848106` | identical |
| distinct_ids | `848098` | `848098` | identical |
| id_checksum | `1820951212451167` | `1820951212451167` | identical |
| payload_bytes | `2437894013` | `2437894013` | identical |
| source_files | `6` | `6` | identical |

## Source duplicates are preserved, not a failure

Bronze holds **8** more rows than distinct `event_id`s. That is correct: the source emits a small number of events twice within a single hourly file, and bronze's job is to reproduce what arrived. Silver dedupes on `event_id`, and the uniqueness test there passes. This figure is identical across both passes, which is itself part of the proof -- a reprocess that changed it would mean rows were being created or dropped.

## Metadata is expected to change

| Measure | Pass 1 | Pass 2 |
|---|---|---|
| Snapshot id | `8351113072006379267` | `3576205085657635040` |
| Operation | `overwrite` | `overwrite` |
| Data files | 12 | 12 |

A new snapshot id is correct: the second write is a second commit. Iceberg keeps both, so the pre-reprocess state stays queryable by snapshot for the retention window.

## Timing

| Pass | Wall time |
|---|---|
| 1 (initial load) | 511.9s |
| 2 (reprocess) | 6482.7s |

**Result: IDEMPOTENT**
