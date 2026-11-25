# Data contract: `iceberg.gold`

What consumers of the gold layer can rely on, and what they cannot. If a
change would break anything stated here, it is a breaking change and needs
a version bump and a notice — not a quiet deploy.

**Owner:** data engineering
**Source of truth:** [GH Archive](https://www.gharchive.org/)
**Serving engine:** Trino over Apache Iceberg

---

## Tables

### `gold.fct_events`

One row per public GitHub event.

| Property | Guarantee |
|---|---|
| Grain | One row per `event_id`. Enforced by `unique` + `not_null` tests. |
| Partitioning | `day(event_ts)`. Always filter on `event_ts` or `event_date`. |
| Clustering | Sorted by `repo_id` within each partition. |
| Late data | Absorbed for **3 days** after the event date via a MERGE on `event_id`. Beyond that window, a reprocess is required. |
| Deletes | Append/update only. Rows are never removed except by an explicit backfill. |

### `gold.fct_repo_daily`

Daily rollup per repository. **Use this, not `fct_events`, for dashboards.**

| Property | Guarantee |
|---|---|
| Grain | One row per (`repo_id`, `event_date`). |
| Freshness | Yesterday's row exists by 04:00 UTC. |
| Zero rows | A repo with no activity has **no row**, not a zero row. Outer-join against a date spine if you need dense series. |

### `gold.dim_repo`

Slowly-changing (Type 2) repository dimension.

| Property | Guarantee |
|---|---|
| Grain | One row per (`repo_id`, `valid_from`). |
| Current row | `is_current = true`. Exactly one per `repo_id`, asserted by `dbt/tests/assert_one_current_repo_row.sql`. |
| Timeline | No gaps or overlaps, asserted by `dbt/tests/assert_no_scd_gaps.sql`. |
| Open interval | `valid_to = 9999-12-31` for current rows, never `NULL`. |

To join facts to the dimension **as it was at the time of the event**:

```sql
SELECT f.event_id, d.repo_name, d.org_login
FROM gold.fct_events f
JOIN gold.dim_repo  d
  ON  f.repo_id  = d.repo_id
  AND f.event_ts >= d.valid_from
  AND f.event_ts <  d.valid_to
WHERE f.event_date = DATE '2026-11-01';
```

Joining on `is_current` instead gives you today's name attached to last
year's events. That is sometimes what you want. It is usually a bug.

### `gold.dim_actor`

**Type 1 by deliberate choice** — overwritten in place, no history. The
event stream carries no reliable account-rename signal, so a Type 2 actor
dimension would be fiction. Documented here so nobody "fixes" it later.

---

## Service levels

| Measure | Target | Enforced by |
|---|---|---|
| Raw landed | Within 1 hour of the source hour closing | `gharchive_ingest` task SLA |
| Gold freshness | Yesterday complete by 04:00 UTC | `gharchive_transform` schedule |
| Silver freshness | Max 24 h since last processed row | `quality_gate.check_freshness` |
| Quarantine rate | Under 0.5 % of rows | `quality_gate.check_quarantine` |
| Completeness | All 24 hours of a day present before transform | `ExternalTaskSensor` |

---

## Known gotchas

These are properties of the source, not bugs. Each one is a wrong number
waiting to happen.

1. **`WatchEvent` means starred, not watching.** Exposed as `is_star`.
2. **`PushEvent.commits` is truncated by the source at exactly 20 commits.**
   `fct_events.commit_count` carries the true total from `payload.size`.
   Counting rows in `silver.commits` undercounts, and the undercount is
   invisible unless you know to look for it.

   Measured on 2023-06-15 hour 00 (145,538 events, 89,273 pushes):

   | Measure | Value |
   |---|---|
   | True commits, `sum(payload.size)` | 244,660 |
   | Rows in `silver.commits` | 135,475 |
   | **Undercount** | **109,185 (44.63%)** |
   | Pushes exceeding 20 commits | 888 |
   | Largest push | 1,000 commits, 20 rows retrieved |

   The cap of 20 is empirical, not documented: the largest push in this hour
   reported 1,000 commits and yielded exactly 20 array entries. Treat
   `silver.commits` as a **sample** of commits, never a census.
3. **Hours are genuinely missing upstream.** GitHub-side outages leave
   holes. These are recorded as `missing_upstream` in `ingest_manifest` and
   are not failures. A dip in `fct_repo_daily` may be a real source gap —
   check the manifest before investigating the pipeline.
4. **The source format changed on 2015-01-01.** Rows carry `source_format`
   (`v3` or `legacy`). Any analysis spanning that boundary should either
   filter on `source_format` or confirm the field it uses is populated on
   both sides.
5. **Private activity is absent entirely.** This is the public event
   stream. It is not a census of GitHub.
6. **Timestamps are UTC.** Any "commits by hour of day" analysis measures
   UTC, not local time, and will therefore mis-attribute contributor
   working hours across time zones.

---

## Breaking vs non-breaking change

**Breaking** (requires notice): removing or renaming a column, changing a
column's type, changing the grain of a table, changing partitioning in a
way that invalidates existing query predicates, narrowing the late-data
window.

**Non-breaking**: adding a column, adding a table, adding a test, changing
file layout or compaction settings, widening the late-data window.

---

## Retention policy

Tiered, because processing volume and storage footprint are different
problems. You do not have to store data to have processed it.

| Zone | Retention | Rationale |
|---|---|---|
| Raw (`s3://.../raw/`) | **Expired once bronze is provably lossless**, minimum age 48 h (168 h in the DAG) | Redundant once every top-level source key is persisted in bronze. Until then it is the only copy of something. |
| Bronze | Indefinite | The replay layer. Holds `payload` whole, so a parser fix re-derives silver without re-downloading. |
| Silver | Indefinite | Typed, column-pruned, zstd. An order of magnitude smaller than raw. |
| Gold | Indefinite | Smallest layer; what consumers read. |
| Iceberg snapshots | 7 days | Time travel is not free. A deliberate trade against unlimited rollback. |

### Raw expiry is gated, not scheduled

`spark/jobs/expire_raw.py` deletes an hour's raw object only when all four
hold:

1. **Age** — older than `--min-age-hours`. A bug found tomorrow should still
   be fixable from source.
2. **Bronze coverage** — rows exist in bronze whose `source_file` is that
   object.
3. **Silver coverage** — the hour cleared normalisation and the quality gate.
4. **Losslessness** — no row in the hour reports an unmapped top-level key.

Gate 4 is the one that matters. `bronze.gh_events.unmapped_keys` records any
top-level source key the schema does not persist. While that array is
non-empty, bronze is **not** a lossless re-encoding of those records, raw is
still the only copy, and expiry is refused. Discarding a field therefore
requires an explicit `--discard-keys` argument, recorded in
`docs/_measured/retention.md`.

The job is **dry-run by default**, and the Airflow task is deliberately not
armed with `--apply`.

### Compaction: where it helps, and where it is a no-op

Measured over 6 hours of 2023-06-15 (848,106 bronze rows):

| Table | Partitioning | Before | After |
|---|---|---|---|
| `bronze.gh_events` | `(ingest_date, ingest_hour)` | 6 files, avg 46.8 MB | **unchanged — 0 rewritten** |
| `silver.events` | `days(event_ts)` | 6 files, avg 3.8 MB | 1 file, 25.7 MB |
| `silver.commits` | `days(event_ts)` | 6 files, avg 9.1 MB | 1 file, 56.6 MB |

**Bronze has nothing to compact, by construction.** One source file maps to
one `(ingest_date, ingest_hour)` partition, so every partition holds exactly
one appropriately-sized file. The small-file problem arises when many writes
land in the same partition — which is what happens in silver, where six
hourly writes all fall inside one `days(event_ts)` partition.

So compaction is a silver-and-gold concern here, not a bronze one. Running
it on bronze is harmless and correctly does nothing.

**Compaction grew total bytes**, by 13% on `silver.events` and 4% on
`silver.commits`. That is expected and is the trade being made: it optimises
**read planning** — one file footer to open instead of six — not storage
size. Per-hour files had better dictionary and encoding locality than the
merged file. Record counts are asserted identical before and after; the job
fails if they move.

### Bytes processed survives expiry

`ingest_manifest.size_bytes` is never cleared when an object is deleted.
Once raw is gone it is the only record of how much source data moved through
the pipeline, so it is the ledger behind every volume claim. See
`tests/report_throughput.py`, which also states which claims the numbers do
**not** support.

### In production this would be a lifecycle rule

On real S3 you would expire raw with an S3 Lifecycle policy rather than a
Spark job — cheaper, and no compute to schedule. The explicit job exists
here because a lifecycle rule expires on **age alone**, with no idea whether
anything downstream actually captured the data. The gated job trades
operational simplicity for the guarantee that nothing is deleted before it
is provably redundant.

---

## Replaying quarantined rows

Quarantined rows retain their original payload, so a parsing fix does not
require re-downloading from the source:

```sql
SELECT reason, count(*)
FROM silver.quarantine
WHERE ingest_date = DATE '2026-11-01'
GROUP BY reason ORDER BY 2 DESC;
```

Fix the parser in `spark/jobs/silver_events.py`, re-run silver for the
affected range, and the MERGE on `event_id` makes the replay safe to
repeat.
