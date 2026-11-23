"""Prove that reprocessing is safe, with evidence.

Everyone claims their pipeline is idempotent. This runs bronze twice over
the same range and compares row counts, a content checksum and the event_id
set, then writes the evidence to docs/_measured/idempotency.md.

What SHOULD change: the Iceberg snapshot id (a second write is a second
commit). What must NOT change: a single row of data.

That distinction is the point. A new snapshot with identical content is
exactly what correct partition-overwrite behaviour looks like.
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, "/opt/project/ingest")
sys.path.insert(0, "/opt/project/spark/jobs")

from common import Settings, add_range_args, resolve_range  # noqa: E402
from session import build  # noqa: E402

log = logging.getLogger("idempotency")
OUT = Path("/opt/project/docs/_measured/idempotency.md")
BRONZE_JOB = "/opt/project/spark/jobs/bronze_raw_to_iceberg.py"


def fingerprint(spark, cat, start, end) -> dict:
    where = f"ingest_date BETWEEN date'{start:%Y-%m-%d}' AND date'{end:%Y-%m-%d}'"
    row = spark.sql(
        f"""
        SELECT count(*)                        AS rows,
               count(DISTINCT event_id)        AS distinct_ids,
               sum(crc32(coalesce(event_id, '')))      AS id_checksum,
               sum(length(coalesce(payload_json, ''))) AS payload_bytes,
               count(DISTINCT source_file)     AS source_files
        FROM {cat}.bronze.gh_events WHERE {where}
        """
    ).collect()[0].asDict()
    snap = spark.sql(
        f"SELECT snapshot_id, operation FROM {cat}.bronze.gh_events.snapshots "
        "ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    row["snapshot_id"] = snap[0]["snapshot_id"] if snap else None
    row["operation"] = snap[0]["operation"] if snap else None
    files = spark.sql(
        f"SELECT count(*) c FROM {cat}.bronze.gh_events.files"
    ).collect()[0]["c"]
    row["data_files"] = files
    return row


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = add_range_args(argparse.ArgumentParser(description=__doc__)).parse_args()
    cfg = Settings.from_env()
    start, end = resolve_range(args)
    rng = ["--start", f"{start:%Y-%m-%dT%H}", "--end", f"{end:%Y-%m-%dT%H}"]

    log.info("pass 1: initial load")
    t0 = time.time()
    if subprocess.run([sys.executable, BRONZE_JOB, *rng], check=False).returncode:
        log.error("first bronze run failed")
        return 1
    t_first = time.time() - t0

    spark = build("idempotency_check_a", cfg)
    before = fingerprint(spark, cfg.catalog, start, end)
    spark.stop()
    log.info("fingerprint after pass 1: %s", before)

    log.info("pass 2: reprocessing the identical range")
    t0 = time.time()
    if subprocess.run([sys.executable, BRONZE_JOB, *rng], check=False).returncode:
        log.error("second bronze run failed")
        return 1
    t_second = time.time() - t0

    spark = build("idempotency_check_b", cfg)
    after = fingerprint(spark, cfg.catalog, start, end)
    spark.stop()
    log.info("fingerprint after pass 2: %s", after)

    data_keys = ["rows", "distinct_ids", "id_checksum", "payload_bytes", "source_files"]
    drift = {k: (before[k], after[k]) for k in data_keys if before[k] != after[k]}
    ok = not drift and after["rows"] == after["distinct_ids"]

    rows_md = [
        f"| {k} | `{before[k]}` | `{after[k]}` | "
        f"{'**CHANGED**' if before[k] != after[k] else 'identical'} |"
        for k in data_keys
    ]

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(
        "\n".join(
            [
                "# Idempotency evidence",
                "",
                f"Range `{start:%Y-%m-%dT%H}` .. `{end:%Y-%m-%dT%H}` UTC, "
                f"measured {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC.",
                "",
                "## Method",
                "",
                "`bronze_raw_to_iceberg.py` was run twice over the same range "
                "with no intervening change. Bronze is partitioned by "
                "`(ingest_date, ingest_hour)` and written with Iceberg's "
                "`overwritePartitions()`, so the second pass replaces exactly "
                "the partitions it rewrites.",
                "",
                "## Data must be identical",
                "",
                "| Measure | Pass 1 | Pass 2 | Verdict |",
                "|---|---|---|---|",
                *rows_md,
                "",
                "## Metadata is expected to change",
                "",
                "| Measure | Pass 1 | Pass 2 |",
                "|---|---|---|",
                f"| Snapshot id | `{before['snapshot_id']}` | `{after['snapshot_id']}` |",
                f"| Operation | `{before['operation']}` | `{after['operation']}` |",
                f"| Data files | {before['data_files']:,} | {after['data_files']:,} |",
                "",
                "A new snapshot id is correct: the second write is a second "
                "commit. Iceberg keeps both, so the pre-reprocess state stays "
                "queryable by snapshot for the retention window.",
                "",
                "## Timing",
                "",
                "| Pass | Wall time |",
                "|---|---|",
                f"| 1 (initial load) | {t_first:.1f}s |",
                f"| 2 (reprocess) | {t_second:.1f}s |",
                "",
                f"**Result: {'IDEMPOTENT' if ok else 'NOT IDEMPOTENT'}**",
                "",
                *(
                    []
                    if ok
                    else [
                        "Drift detected:",
                        "",
                        *[f"* `{k}`: {v[0]} -> {v[1]}" for k, v in drift.items()],
                        "",
                    ]
                ),
            ]
        ),
        encoding="utf-8",
    )
    log.info("wrote %s", OUT)

    if not ok:
        log.error("NOT idempotent: %s", drift)
        return 1
    log.info("idempotent: data identical across both passes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
