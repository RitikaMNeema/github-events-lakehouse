"""Table maintenance: compact small files, rewrite manifests, expire snapshots.

Streaming and hourly batch both produce many small files. Small files are
the single most common reason a lakehouse query is slow for no apparent
reason: the planner spends its time opening footers instead of reading data.

This job measures the table before and after, so the improvement is a
number you can put in a document rather than an assertion.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, "/opt/project/ingest")
sys.path.insert(0, "/opt/project/spark/jobs")

from common import Settings  # noqa: E402
from session import build  # noqa: E402

log = logging.getLogger("compact")
OUT_DIR = Path("/opt/project/docs/_measured")


def file_stats(spark, fq_table: str) -> dict:
    row = spark.sql(
        f"""
        SELECT count(*)                         AS file_count,
               sum(file_size_in_bytes)          AS total_bytes,
               cast(avg(file_size_in_bytes) AS bigint) AS avg_bytes,
               min(file_size_in_bytes)          AS min_bytes,
               max(file_size_in_bytes)          AS max_bytes,
               sum(record_count)                AS records
        FROM {fq_table}.files
        """
    ).collect()[0]
    return {k: (row[k] or 0) for k in row.asDict()}


def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} PB"


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--table", default="bronze.gh_events", help="namespace.table")
    ap.add_argument("--target-file-mb", type=int, default=128)
    ap.add_argument("--min-input-files", type=int, default=5)
    ap.add_argument("--retain-hours", type=int, default=168,
                    help="snapshot retention; time-travel window")
    ap.add_argument("--no-expire", action="store_true")
    args = ap.parse_args()

    cfg = Settings.from_env()
    spark = build("compaction", cfg)
    fq = f"{cfg.catalog}.{args.table}"

    before = file_stats(spark, fq)
    log.info(
        "before: %s files, avg %s, total %s",
        f"{before['file_count']:,}", human(before["avg_bytes"]), human(before["total_bytes"]),
    )

    target_bytes = args.target_file_mb * 1024 * 1024
    res = spark.sql(
        f"""
        CALL {cfg.catalog}.system.rewrite_data_files(
            table   => '{args.table}',
            options => map(
                'target-file-size-bytes', '{target_bytes}',
                'min-input-files',        '{args.min_input_files}',
                'partial-progress.enabled','true'
            )
        )
        """
    ).collect()[0]
    log.info(
        "rewrote %s files into %s",
        res["rewritten_data_files_count"], res["added_data_files_count"],
    )

    # Manifest rewrite keeps planning cheap as snapshot count grows.
    spark.sql(f"CALL {cfg.catalog}.system.rewrite_manifests(table => '{args.table}')")

    if not args.no_expire:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=args.retain_hours)
        spark.sql(
            f"""
            CALL {cfg.catalog}.system.expire_snapshots(
                table      => '{args.table}',
                older_than => TIMESTAMP '{cutoff:%Y-%m-%d %H:%M:%S}',
                retain_last=> 5
            )
            """
        )
        log.info("expired snapshots older than %s (kept at least 5)", cutoff)

    after = file_stats(spark, fq)
    log.info(
        "after:  %s files, avg %s, total %s",
        f"{after['file_count']:,}", human(after["avg_bytes"]), human(after["total_bytes"]),
    )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUT_DIR / f"compaction_{args.table.replace('.', '_')}.md"
    rows = [
        ("Data files", f"{before['file_count']:,}", f"{after['file_count']:,}"),
        ("Avg file size", human(before["avg_bytes"]), human(after["avg_bytes"])),
        ("Smallest file", human(before["min_bytes"]), human(after["min_bytes"])),
        ("Largest file", human(before["max_bytes"]), human(after["max_bytes"])),
        ("Total size", human(before["total_bytes"]), human(after["total_bytes"])),
        ("Records", f"{before['records']:,}", f"{after['records']:,}"),
    ]
    body = [
        f"# Compaction: `{args.table}`",
        "",
        f"Measured {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC, "
        f"target file size {args.target_file_mb} MB.",
        "",
        "| Metric | Before | After |",
        "|---|---|---|",
        *[f"| {a} | {b} | {c} |" for a, b, c in rows],
        "",
        "Record count must be identical before and after. Compaction rewrites "
        "file layout only; if this number moves, the job has a bug.",
        "",
    ]
    out.write_text("\n".join(body), encoding="utf-8")
    log.info("wrote %s", out)

    if before["records"] != after["records"]:
        log.error("RECORD COUNT CHANGED during compaction - investigate")
        spark.stop()
        return 1

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
