"""Partition-layout benchmark: does the physical design actually pay off?

Builds the same rows three ways and runs the same queries against each:

  unpartitioned     one flat table
  day_partitioned   PARTITIONED BY days(event_ts)
  day_sorted        day-partitioned AND sorted by repo_id within files

Reports wall time and, more importantly, how many data files each query
can prune to. Wall time on a laptop is noisy; files pruned is a structural
property of the layout and is the number worth defending in an interview.

Method limits, stated up front: single node, warm OS cache, small data
volume. The ORDER of the results generalises; the magnitudes do not.
"""
from __future__ import annotations

import argparse
import logging
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, "/opt/project/ingest")
sys.path.insert(0, "/opt/project/spark/jobs")

from common import Settings  # noqa: E402
from session import build  # noqa: E402

log = logging.getLogger("benchmark")
OUT = Path("/opt/project/docs/_measured/partitioning_benchmark.md")
NS = "bench"

LAYOUTS = {
    "unpartitioned": "",
    "day_partitioned": "PARTITIONED BY (days(event_ts))",
    "day_sorted": "PARTITIONED BY (days(event_ts))",
}


def build_tables(spark, cat: str, source: str, target_mb: int) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.{NS}")
    props = (
        f"TBLPROPERTIES ('write.target-file-size-bytes'='{target_mb * 1024 * 1024}',"
        " 'write.format.default'='parquet',"
        " 'write.parquet.compression-codec'='zstd')"
    )
    for name, part in LAYOUTS.items():
        tbl = f"{cat}.{NS}.{name}"
        spark.sql(f"DROP TABLE IF EXISTS {tbl}")
        spark.sql(
            f"""CREATE TABLE {tbl} USING iceberg {part} {props}
                AS SELECT event_id, event_ts, event_type, actor_id, repo_id,
                          repo_name, action, commit_count
                   FROM {source} WHERE event_ts IS NOT NULL"""
        )
        if name == "day_sorted":
            # Sorting within files makes repo_id range stats narrow, so the
            # planner can skip files on a repo filter, not just a date one.
            spark.sql(f"ALTER TABLE {tbl} WRITE ORDERED BY repo_id")
            spark.sql(
                f"CALL {cat}.system.rewrite_data_files("
                f"table => '{NS}.{name}', strategy => 'sort', "
                f"sort_order => 'repo_id ASC NULLS LAST')"
            )
        n = spark.sql(f"SELECT count(*) c FROM {tbl}").collect()[0]["c"]
        f = spark.sql(f"SELECT count(*) c FROM {tbl}.files").collect()[0]["c"]
        log.info("%-16s %s rows across %s files", name, f"{n:,}", f)


def time_query(spark, sql: str, repeats: int) -> float:
    """Median wall time over `repeats` runs, first run discarded as warm-up."""
    times = []
    spark.sql(sql).collect()  # warm-up
    for _ in range(repeats):
        t0 = time.perf_counter()
        spark.sql(sql).collect()
        times.append(time.perf_counter() - t0)
    return statistics.median(times)


def files_touched(spark, cat: str, name: str, predicate: str | None) -> int:
    """How many data files survive partition pruning for this predicate."""
    q = f"SELECT count(*) c FROM {cat}.{NS}.{name}.files"
    if predicate:
        q += f" WHERE {predicate}"
    return spark.sql(q).collect()[0]["c"]



def _pruning_caveat(lo, hi) -> str:
    """Say plainly when the data slice cannot demonstrate partition pruning.

    A day-partitioned table built from a single day of data has exactly ONE
    partition, so a single-day predicate selects everything and prunes
    nothing. Any speedup measured in that situation comes from file layout,
    not from pruning -- and presenting it as a pruning result would be
    straightforwardly misleading.
    """
    days = (hi.date() - lo.date()).days + 1
    if days >= 2:
        return (
            f"> The slice spans **{days} days**, so the single-day predicate "
            "genuinely prunes partitions and the files-pruned column below is "
            "meaningful.\n\n"
        )
    return (
        "> ## This slice CANNOT demonstrate partition pruning\n"
        ">\n"
        f"> The data spans a single day ({lo.date()}), so the day-partitioned\n"
        "> table has exactly **one** partition and the single-day predicate\n"
        "> selects all of it. Nothing is pruned.\n"
        ">\n"
        "> The timing differences below are therefore attributable to file\n"
        "> layout and file count, **not** to partition pruning. To measure\n"
        "> pruning, ingest at least two days (widen `GHA_END` in `.env`) and\n"
        "> re-run. Until then, treat the partitioned-vs-unpartitioned gap as\n"
        "> suggestive, not as evidence.\n\n"
    )

def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--target-file-mb", type=int, default=64)
    ap.add_argument("--rebuild", action="store_true", default=True)
    args = ap.parse_args()

    cfg = Settings.from_env()
    cat = cfg.catalog
    spark = build("benchmark_layout", cfg)
    source = f"{cat}.silver.events"

    bounds = spark.sql(
        f"SELECT min(event_ts) lo, max(event_ts) hi, count(*) n FROM {source}"
    ).collect()[0]
    if not bounds["n"]:
        log.error("silver.events is empty - run `make ingest bronze silver` first")
        return 1
    day = bounds["hi"].strftime("%Y-%m-%d")
    log.info("source holds %s rows, %s .. %s", f"{bounds['n']:,}", bounds["lo"], bounds["hi"])

    if args.rebuild:
        build_tables(spark, cat, source, args.target_file_mb)

    queries = {
        "single-day scan": (
            f"SELECT count(*) FROM {{t}} WHERE event_ts >= timestamp'{day} 00:00:00' "
            f"AND event_ts < timestamp'{day} 23:59:59'",
            None,
        ),
        "day + group by type": (
            f"SELECT event_type, count(*) FROM {{t}} "
            f"WHERE event_ts >= timestamp'{day} 00:00:00' GROUP BY event_type",
            None,
        ),
        "full-table aggregate": (
            f"SELECT event_type, count(*) FROM {{t}} GROUP BY event_type",
            None,
        ),
    }

    results: dict[str, dict[str, float]] = {}
    for name in LAYOUTS:
        tbl = f"{cat}.{NS}.{name}"
        results[name] = {}
        for label, (tmpl, _) in queries.items():
            secs = time_query(spark, tmpl.format(t=tbl), args.repeats)
            results[name][label] = secs
            log.info("%-16s %-22s %.3fs", name, label, secs)

    layout_meta = {}
    for name in LAYOUTS:
        total = files_touched(spark, cat, name, None)
        pruned = files_touched(spark, cat, name, f"partition.event_ts_day = date'{day}'") \
            if name != "unpartitioned" else total
        sizes = spark.sql(
            f"SELECT cast(avg(file_size_in_bytes)/1048576 AS decimal(10,2)) avg_mb "
            f"FROM {cat}.{NS}.{name}.files"
        ).collect()[0]["avg_mb"]
        layout_meta[name] = {"files": total, "pruned": pruned, "avg_mb": sizes}

    hdr = "| Query | " + " | ".join(LAYOUTS) + " |"
    sep = "|---" * (len(LAYOUTS) + 1) + "|"
    body = [
        f"| {label} | "
        + " | ".join(f"{results[n][label]:.3f}s" for n in LAYOUTS)
        + " |"
        for label in queries
    ]

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(
        "\n".join(
            [
                "# Partition layout benchmark",
                "",
                f"Measured {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC on "
                f"{bounds['n']:,} rows, target file size {args.target_file_mb} MB, "
                f"median of {args.repeats} runs after a discarded warm-up.",
                "",
                "## Layouts compared",
                "",
                "| Layout | Partitioning | Data files | Files after date prune | Avg file size |",
                "|---|---|---|---|---|",
                *[
                    f"| `{n}` | {LAYOUTS[n] or 'none'}"
                    + (" + sorted by repo_id" if n == "day_sorted" else "")
                    + f" | {layout_meta[n]['files']:,} | {layout_meta[n]['pruned']:,} "
                    f"| {layout_meta[n]['avg_mb']} MB |"
                    for n in LAYOUTS
                ],
                "",
                "## Query wall time",
                "",
                hdr,
                sep,
                *body,
                "",
                _pruning_caveat(bounds["lo"], bounds["hi"]),
                "## Reading these numbers",
                "",
                "* **Files after date prune** is the structural result. The "
                "unpartitioned table must open every file to answer a "
                "single-day question; the partitioned tables open only the "
                "day's files. That ratio holds at any scale.",
                "* **Wall time** is single-node and cache-warm. The direction "
                "is meaningful, the magnitude is not: at this data volume a "
                "full scan is cheap, so pruning looks less dramatic than it "
                "is on a real cluster.",
                "* **`day_sorted`** costs more to write (an extra sort-rewrite "
                "pass) and only pays off on queries that filter by `repo_id`. "
                "It is the right default only if that is the access pattern - "
                "which is why the production tables declare it explicitly "
                "rather than inheriting it.",
                "* **Full-table aggregate** should be roughly equal across all "
                "three. If partitioning appears to speed up a query that reads "
                "everything, the measurement is wrong.",
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
