# Postmortem: injected data-corruption drill

> This was a **deliberate drill**, not a production incident. It is written in incident format because the point of the exercise is to find out whether the controls work before a real one arrives.

| | |
|---|---|
| Drill run | 2026-11-25 17:00 UTC |
| Range affected | `2023-06-15T00` .. `2023-06-15T05` |
| Corrupt rows injected | 1500 |
| Rows quarantined | 1000 |
| Quality gate | FAILED the run (correct) |
| Silver job exit code | 0 |

## What was injected

| Defect | Intent |
|---|---|
| `null_event_id` | a row with no primary key |
| `future_timestamp` | created_at a year in the future |
| `unparseable_timestamp` | created_at that is not a timestamp |
| `duplicate_event_id` | the same event_id twice, to test the MERGE key |
| `null_event_type` | no event type |

All rows were written to a sentinel partition All rows were tagged `source_file = s3a://drill/injected`, so every injected row is removable by one predicate while still being visible to the pipeline. An earlier version used an impossible partition value instead, which bounded the blast radius perfectly and made the drill test nothing at all.

## What the pipeline did

### Quarantine reasons recorded

| Reason | Rows |
|---|---|
| null event_type | 250 |
| event_ts in the future | 250 |
| null event_id | 250 |
| unparseable created_at | 250 |

### Quality gate output

```
ERROR ERROR quarantine rate: 1,000 quarantined of 146,788 (0.681%), threshold 0.50%
WARNING WARN  schema drift: 1 unmapped top-level key(s) present: drill_unmapped_field - bronze is NOT lossless for these rows, so raw expiry is blocked until they are mapped or explicitly discarded
```

## Detection

The corrupt rows never reached the gold layer. Silver validation diverted them to `silver.quarantine` with a reason and the original payload attached, and the quality gate exited non-zero, which fails the Airflow task and stops the downstream dbt build.

## What this drill shows works

* Bad rows are **diverted, not dropped**. The payload is retained in quarantine, so the rows can be replayed once the parsing bug is fixed - no re-download required.
* The `event_id` MERGE key absorbed a duplicate without creating a second row in `silver.events`.
* Blast radius was one partition, and recovery was one `DELETE FROM ... WHERE ingest_hour = 99`.

## What this drill shows is weak

* Detection was **synchronous with the run**. Nothing here would catch corruption that arrives between scheduled runs; that needs the freshness check on a separate schedule from the pipeline that produces the data.
* The volume-anomaly check is a median +/- tolerance band. It will not catch a slow drift that moves the median with it.
* Quarantine has no alerting. It writes a table and a report; nobody is paged. Wiring it to a real channel is the obvious next step.

## Cleanup

Sentinel partition removed; quality gate re-run exited `0`.
