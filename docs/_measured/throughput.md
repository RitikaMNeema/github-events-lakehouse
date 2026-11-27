# Throughput ledger

Generated 2026-11-26 07:02 UTC from `ingest_manifest` and the Iceberg metadata tables.

## Source volume processed

| Measure | Value |
|---|---|
| Compressed source bytes processed | **2.1 GB** |
| Hourly batches | 30 |
| Date span | `2023-06-15 00:00:00+00:00` .. `2023-06-16 05:00:00+00:00` |
| Upstream gaps (source outages) | 0 |
| Fetch failures | 0 |
| Hours with raw expired | 1 |
| Bytes reclaimed by expiry | 62.0 MB |

## Retained footprint

| Table | Rows | On disk | Files |
|---|---|---|---|
| `bronze.gh_events` | 4,649,119 | 1.7 GB | 30 |
| `silver.events` | 1,669,055 | 51.0 MB | 2 |
| `silver.commits` | 1,498,847 | 108.7 MB | 7 |
| `silver.quarantine` | 0 | 0.0 B | 0 |
| **total retained** | | **1.8 GB** | |

Retention ratio: **87.1%** of source volume.

`bronze.gh_events` alone holds **92%** of retained bytes. That is a deliberate choice, not a compression failure: bronze keeps `payload` whole so a parser fix can re-derive silver without re-ingesting.

The typed, column-pruned layer tells the real compression story: `silver.events` is **51.0 MB** for the same rows, **43x smaller** than the compressed source.

Which means the retention levers rank:

1. **Expire raw** once bronze is provably lossless -- frees the full source volume.
2. **Then bronze is the next target**, being the largest remaining layer. Tiering it (keep `payload_json` hot for N days, then drop the column and rely on silver) is the real saving at scale, and the real trade: it ends unlimited replay.
3. Compression and column pruning are already doing their job in silver; there is little left to win there.

## Claims these measurements support

* Streamed and processed **2.1 GB** of compressed source JSON across **30 hourly batches** (1.2 days of GitHub activity).
* Normalised **1,669,055 events** into a typed silver layer.
* Retained **1.8 GB** (87.1% of source volume) after column pruning and zstd, under a documented raw-expiry retention policy.
* Reclaimed **62.0 MB** by expiring raw objects for **1 hours** that passed a four-gate losslessness check.

## Claims they do NOT support

* **Do not say you "stored" or "managed" 2.1 GB.** You processed it; you retain 1.8 GB. Say *processed*.
* **Do not claim distributed-cluster scale.** This ran in Spark local mode on one machine. Say "single-node" or say nothing about the runtime.
* **Do not claim a cost saving.** No bill was incurred. The README gives a cost *model*; say "modelled cost drivers".
* **Do not quote throughput in MB/s** from this report. It measures volume, not rate, and your rate was bounded by home bandwidth.
* **Do not claim a gold/dimensional layer.** `gold.fct_events` does not exist yet -- the dbt build has not run.
