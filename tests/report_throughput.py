"""The bytes-processed ledger, and the sentences it entitles you to say.

Once raw is expired, `ingest_manifest.size_bytes` is the only surviving
record of how much source data moved through the pipeline. This reads that
ledger plus the Iceberg metadata tables and writes a report that states,
explicitly, which claims the measurements support and which they do not.

That second list exists because the fastest way to fail a technical screen
is a number you cannot source. A report that tells you what you may NOT
claim is worth more than one that flatters you.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, "/opt/project/ingest")
sys.path.insert(0, "/opt/project/spark/jobs")

from common import Settings, pg_conn  # noqa: E402
from session import build  # noqa: E402

log = logging.getLogger("throughput")
OUT = Path("/opt/project/docs/_measured/throughput.md")

COUNTED = ("fetched", "present", "raw_expired")


def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(n) < 1024:
            return f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} EB"


def ledger(cfg: Settings) -> dict:
    with pg_conn(cfg) as c, c.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                count(*) FILTER (WHERE status = ANY(%s))                AS hours_processed,
                coalesce(sum(size_bytes) FILTER (WHERE status = ANY(%s)), 0) AS bytes_processed,
                count(*) FILTER (WHERE status = 'raw_expired')          AS hours_expired,
                coalesce(sum(size_bytes) FILTER (WHERE status = 'raw_expired'), 0) AS bytes_expired,
                count(*) FILTER (WHERE status = 'missing_upstream')     AS hours_missing,
                count(*) FILTER (WHERE status = 'failed')               AS hours_failed,
                min(hour_ts) AS first_hour,
                max(hour_ts) AS last_hour
            FROM ingest_manifest
            """,
            (list(COUNTED), list(COUNTED)),
        )
        keys = [
            "hours_processed", "bytes_processed", "hours_expired", "bytes_expired",
            "hours_missing", "hours_failed", "first_hour", "last_hour",
        ]
        return dict(zip(keys, cur.fetchone()))


def table_stats(spark, fq: str) -> dict | None:
    try:
        r = spark.sql(
            f"""SELECT coalesce(sum(record_count), 0) rows,
                       coalesce(sum(file_size_in_bytes), 0) bytes,
                       count(*) files
                FROM {fq}.files"""
        ).collect()[0]
        return {"rows": r["rows"], "bytes": r["bytes"], "files": r["files"]}
    except Exception:
        return None


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    argparse.ArgumentParser(description=__doc__).parse_args()

    cfg = Settings.from_env()
    spark = build("report_throughput", cfg)
    cat = cfg.catalog

    L = ledger(cfg)
    if not L["hours_processed"]:
        log.error("ledger is empty - run `make ingest` first")
        spark.stop()
        return 1

    tables = {
        name: table_stats(spark, f"{cat}.{name}")
        for name in (
            "bronze.gh_events",
            "silver.events",
            "silver.commits",
            "silver.quarantine",
            "gold.fct_events",
            "gold.fct_repo_daily",
        )
    }
    present = {k: v for k, v in tables.items() if v}
    retained = sum(v["bytes"] for v in present.values())
    processed = L["bytes_processed"]
    span_days = max(
        (L["last_hour"] - L["first_hour"]).total_seconds() / 86400, 0
    ) if L["first_hour"] else 0

    retention_pct = (retained / processed * 100) if processed else 0
    events = present.get("silver.events", {}).get("rows", 0)

    log.info(
        "processed %s across %s hours; retaining %s (%.1f%%)",
        human(processed), L["hours_processed"], human(retained), retention_pct,
    )

    # ------------------------------------------------------------- claims
    may = [
        f"Streamed and processed **{human(processed)}** of compressed source "
        f"JSON across **{L['hours_processed']:,} hourly batches** "
        f"({span_days:.1f} days of GitHub activity).",
    ]
    if events:
        may.append(f"Normalised **{events:,} events** into a typed silver layer.")
    if processed and retained:
        may.append(
            f"Retained **{human(retained)}** ({retention_pct:.1f}% of source "
            f"volume) after column pruning and zstd, under a documented "
            f"raw-expiry retention policy."
        )
    if L["hours_expired"]:
        may.append(
            f"Reclaimed **{human(L['bytes_expired'])}** by expiring raw "
            f"objects for **{L['hours_expired']:,} hours** that passed a "
            f"four-gate losslessness check."
        )
    if L["hours_missing"]:
        may.append(
            f"Handled **{L['hours_missing']:,} upstream gaps** as data rather "
            f"than failures, distinguishing source outages from pipeline errors."
        )

    may_not = [
        f"**Do not say you \"stored\" or \"managed\" {human(processed)}.** You "
        f"processed it; you retain {human(retained)}. Say *processed*.",
        "**Do not claim distributed-cluster scale.** This ran in Spark local "
        "mode on one machine. Say \"single-node\" or say nothing about the "
        "runtime.",
        "**Do not claim a cost saving.** No bill was incurred. The README "
        "gives a cost *model*; say \"modelled cost drivers\".",
        "**Do not quote throughput in MB/s** from this report. It measures "
        "volume, not rate, and your rate was bounded by home bandwidth.",
    ]
    if not L["hours_expired"]:
        may_not.append(
            "**Do not claim raw expiry.** No hours have been expired yet "
            "(`make expire-raw` has not run with `--apply`)."
        )
    if not present.get("gold.fct_events"):
        may_not.append(
            "**Do not claim a gold/dimensional layer.** `gold.fct_events` "
            "does not exist yet -- the dbt build has not run."
        )

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(
        "\n".join(
            [
                "# Throughput ledger",
                "",
                f"Generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC from "
                "`ingest_manifest` and the Iceberg metadata tables.",
                "",
                "## Source volume processed",
                "",
                "| Measure | Value |",
                "|---|---|",
                f"| Compressed source bytes processed | **{human(processed)}** |",
                f"| Hourly batches | {L['hours_processed']:,} |",
                f"| Date span | `{L['first_hour']}` .. `{L['last_hour']}` |",
                f"| Upstream gaps (source outages) | {L['hours_missing']:,} |",
                f"| Fetch failures | {L['hours_failed']:,} |",
                f"| Hours with raw expired | {L['hours_expired']:,} |",
                f"| Bytes reclaimed by expiry | {human(L['bytes_expired'])} |",
                "",
                "## Retained footprint",
                "",
                "| Table | Rows | On disk | Files |",
                "|---|---|---|---|",
                *[
                    f"| `{k}` | {v['rows']:,} | {human(v['bytes'])} | {v['files']:,} |"
                    for k, v in present.items()
                ],
                f"| **total retained** | | **{human(retained)}** | |",
                "",
                f"Retention ratio: **{retention_pct:.1f}%** of source volume. "
                "The gap is column pruning plus zstd on Parquet -- the reason "
                "processing volume and storage footprint are different numbers, "
                "and why only one of them belongs on a resume.",
                "",
                "## Claims these measurements support",
                "",
                *[f"* {c}" for c in may],
                "",
                "## Claims they do NOT support",
                "",
                *[f"* {c}" for c in may_not],
                "",
            ]
        ),
        encoding="utf-8",
    )
    log.info("wrote %s", OUT)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
