# Raw-zone retention

**APPLIED** -- 2026-11-25 23:31 UTC, range `2023-06-15T00` .. `2023-06-15T00`.

## Policy

* Minimum age before expiry: **48h**
* Silver coverage required: **True**
* Keys explicitly approved for discard: **none**

## Result

| Measure | Value |
|---|---|
| Source bytes processed (all time, this range) | 62.0 MB |
| Hours expired | 1 |
| Bytes reclaimed | 62.0 MB |
| Raw still retained | 0.0 B |
| Hours refused | 0 |

## Refusals

| Hour | Size | Why raw was kept |
|---|---|---|
| _none_ | | |

A refusal is the system working. The commonest one is an unmapped top-level key, which means bronze is **not** a lossless re-encoding of those records and the raw object is still the only copy of something. Either map the field in `PERSISTED_KEYS`, or decide out loud to lose it with `--discard-keys`.

## Why bytes_processed survives deletion

`ingest_manifest.size_bytes` is never cleared on expiry. Once the object is gone, that column is the only remaining record of how much source data moved through the pipeline -- so it is the ledger behind any throughput claim. See `tests/report_throughput.py`.
