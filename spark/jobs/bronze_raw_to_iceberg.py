"""Raw gzipped JSON -> Iceberg bronze.

Bronze rules, and why:

1. `payload` is kept as a RAW JSON STRING. Its shape differs per event type
   and has changed over the years. Typing it here would mean rewriting
   bronze every time a new event type appears; typing it in silver means a
   cheap re-derive from data we already hold.

2. Partitioned by INGEST hour, not event time. One source file is one
   (ingest_date, ingest_hour) partition, so `overwritePartitions()`
   replaces exactly that file's rows and nothing else. Reprocessing one bad
   hour is a single atomic snapshot. Event-time partitioning happens in
   silver, where a MERGE can absorb the overlap.

3. The legacy/v3 format break (the source changed shape on 2015-01-01) is
   detected and recorded as `source_format`, not resolved. Bronze records
   what arrived; silver decides what it means.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import timezone

from pyspark.sql import functions as F

sys.path.insert(0, "/opt/project/ingest")
sys.path.insert(0, "/opt/project/spark/jobs")

from common import (  # noqa: E402
    Settings,
    add_range_args,
    hour_range,
    manifest_rows,
    resolve_range,
)
from session import build, raw_path  # noqa: E402

log = logging.getLogger("bronze")

TABLE = "bronze.gh_events"

# Exactly the top-level source keys this schema persists. Anything else a
# record carries is recorded in `unmapped_keys` rather than silently dropped.
#
# This list is the contract that makes raw expiry safe: if it is complete,
# bronze is a lossless re-encoding of the source record and the raw object
# is redundant. If it is NOT complete, deleting raw destroys data nobody
# knows existed. `expire_raw.py` refuses to delete while any row in the
# hour reports an unmapped key.
PERSISTED_KEYS = [
    "id", "_id",          # event id (v3 / legacy)
    "type",
    "actor",
    "repo", "repository",  # v3 / legacy
    "org",
    "payload",
    "public",
    "created_at",
]

_persisted_sql = ", ".join(f"'{k}'" for k in PERSISTED_KEYS)
UNMAPPED_EXPR = (
    "array_except("
    "  coalesce(json_object_keys(value), cast(array() as array<string>)),"
    f"  array({_persisted_sql})"
    ")"
)

DDL = """
CREATE TABLE IF NOT EXISTS {cat}.{tbl} (
    event_id      STRING    COMMENT 'GitHub event id; globally unique, the merge key',
    event_type    STRING    COMMENT 'PushEvent, WatchEvent, ... see docs/data-contract.md',
    created_at    TIMESTAMP COMMENT 'event time, as reported by the source',
    actor_json    STRING    COMMENT 'raw; scalar login in legacy format, object in v3',
    repo_json     STRING    COMMENT 'raw; `repository` in legacy format, `repo` in v3',
    org_json      STRING    COMMENT 'raw; absent for personal repos',
    payload_json  STRING    COMMENT 'raw; shape varies by event_type',
    is_public     BOOLEAN,
    source_format STRING    COMMENT 'v3 | legacy',
    unmapped_keys ARRAY<STRING> COMMENT 'top-level source keys NOT persisted by this schema; non-empty means bronze is lossy for this row and raw MUST NOT be expired',
    source_file   STRING    COMMENT 'provenance: which object this row came from',
    ingest_date   DATE      COMMENT 'partition: date of the source file',
    ingest_hour   INT       COMMENT 'partition: hour of the source file',
    ingested_at   TIMESTAMP COMMENT 'when this row was written'
)
USING iceberg
PARTITIONED BY (ingest_date, ingest_hour)
TBLPROPERTIES (
    'write.format.default'           = 'parquet',
    'write.parquet.compression-codec'= 'zstd',
    'write.target-file-size-bytes'   = '134217728',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max'       = '20'
)
"""


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = add_range_args(argparse.ArgumentParser(description=__doc__))
    args = ap.parse_args()

    cfg = Settings.from_env()
    start, end = resolve_range(args)
    spark = build("bronze_raw_to_iceberg", cfg)

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cfg.catalog}.bronze")
    spark.sql(DDL.format(cat=cfg.catalog, tbl=TABLE))

    # Schema evolution, in anger rather than in theory. A table created
    # before `unmapped_keys` existed gains it here without a rewrite --
    # Iceberg tracks columns by id, so old files simply read back NULL.
    existing = set(spark.table(f"{cfg.catalog}.{TABLE}").schema.fieldNames())
    if "unmapped_keys" not in existing:
        log.info("evolving %s: adding unmapped_keys", TABLE)
        spark.sql(
            f"ALTER TABLE {cfg.catalog}.{TABLE} "
            "ADD COLUMNS (unmapped_keys ARRAY<STRING> COMMENT "
            "'top-level source keys NOT persisted by this schema')"
        )

    # Read only hours the manifest says are actually in the raw zone.
    #
    # spark.read.text() raises PATH_NOT_FOUND on a path that does not exist,
    # so passing every hour in the range crashes the batch the moment it
    # spans a gap -- and GH Archive genuinely has gaps, plus any hour whose
    # raw object has since been expired. A backfill must survive both.
    #
    # The manifest already knows which hours landed, so it is the authority
    # here. `raw_expired` is excluded on purpose: the row was processed once
    # and its object is deliberately gone.
    available = {
        m["hour_ts"].replace(tzinfo=timezone.utc): m
        for m in manifest_rows(cfg, start, end)
        if m["status"] in ("fetched", "present")
    }
    hours = [h for h in hour_range(start, end) if h in available]
    skipped = [h for h in hour_range(start, end) if h not in available]

    if skipped:
        log.warning(
            "skipping %d hour(s) absent from the raw zone (gaps, unfetched or "
            "expired): %s",
            len(skipped),
            ", ".join(f"{h:%m-%dT%H}" for h in skipped[:8]),
        )
    if not hours:
        log.error(
            "no fetched hours in range %s..%s - run ingest first",
            f"{start:%Y-%m-%dT%H}", f"{end:%Y-%m-%dT%H}",
        )
        spark.stop()
        return 1

    log.info("processing %d hour(s), one at a time", len(hours))

    target = f"{cfg.catalog}.{TABLE}"

    # ONE HOUR PER WRITE, deliberately.
    #
    # Reading the whole range in a single pass made peak memory grow with
    # the width of the range: six hours (~2.8 GB of uncompressed JSON) killed
    # a 2 GB driver outright -- "Answer from Java side is empty", i.e. the
    # JVM died rather than a query failing. A job that cannot widen its
    # range without more RAM cannot backfill a year, which is the entire
    # point of the thing.
    #
    # Looping makes peak memory a function of ONE hour, flat regardless of
    # range. It also means each hour is its own Iceberg commit, so a failure
    # half way through a backfill leaves every completed hour intact instead
    # of losing the batch. Slightly more commits; vastly better failure
    # behaviour. This is also exactly what the hourly DAG does per run, so
    # the manual path and the orchestrated path now behave identically.
    for i, h in enumerate(hours, 1):
        path = raw_path(cfg, f"{h:%Y-%m-%d}", f"{h:%H}")
        log.info("[%d/%d] %s", i, len(hours), f"{h:%Y-%m-%dT%H}")

        df_raw = (
            spark.read.option("recursiveFileLookup", "false")
            # Belt and braces: an object deleted between the manifest read
            # and the scan must not fail the batch either.
            .option("ignoreMissingFiles", "true")
            .text(path)
            .withColumn("source_file", F.input_file_name())
        )
        _write_hour(spark, df_raw, target)

    n = spark.sql(
        f"""SELECT count(*) c FROM {target}
            WHERE ingest_date BETWEEN date'{start:%Y-%m-%d}' AND date'{end:%Y-%m-%d}'"""
    ).collect()[0]["c"]
    log.info("bronze now holds %s rows for the requested range", f"{n:,}")

    snap = spark.sql(
        f"SELECT snapshot_id, operation FROM {target}.snapshots "
        "ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    if snap:
        log.info("latest snapshot %s (%s)", snap[0]["snapshot_id"], snap[0]["operation"])

    spark.stop()
    return 0


def _write_hour(spark, df_raw, target: str) -> None:
    """Project one hour's raw lines into bronze and commit that partition."""
    j = F.col("value")
    legacy = F.get_json_object(j, "$.repository").isNotNull()

    bronze = df_raw.select(
        F.coalesce(
            F.get_json_object(j, "$._id"), F.get_json_object(j, "$.id")
        ).alias("event_id"),
        F.get_json_object(j, "$.type").alias("event_type"),
        F.to_timestamp(F.get_json_object(j, "$.created_at")).alias("created_at"),
        F.get_json_object(j, "$.actor").alias("actor_json"),
        # legacy called it `repository`, v3 calls it `repo`
        F.coalesce(
            F.get_json_object(j, "$.repo"), F.get_json_object(j, "$.repository")
        ).alias("repo_json"),
        F.get_json_object(j, "$.org").alias("org_json"),
        F.get_json_object(j, "$.payload").alias("payload_json"),
        (F.get_json_object(j, "$.public") == "true").alias("is_public"),
        F.when(legacy, F.lit("legacy")).otherwise(F.lit("v3")).alias("source_format"),
        # Every top-level key the record carries, minus the ones we persist.
        # Empty array => this row is fully captured and raw is redundant.
        #
        # Expressed as SQL rather than the functions API: json_object_keys
        # has no guaranteed Python wrapper across Spark 3.x, and a bare
        # array() has ambiguous element typing that array_except rejects.
        F.expr(UNMAPPED_EXPR).alias("unmapped_keys"),
        F.col("source_file"),
        # Partition values come from the object path, not the event body, so
        # they are stable even when created_at is garbage.
        F.to_date(
            F.regexp_extract(F.col("source_file"), r"event_date=(\d{4}-\d{2}-\d{2})", 1)
        ).alias("ingest_date"),
        F.regexp_extract(F.col("source_file"), r"hour=(\d{2})", 1)
        .cast("int")
        .alias("ingest_hour"),
        F.current_timestamp().alias("ingested_at"),
    )

    # Dynamic partition overwrite: replaces only the (ingest_date,
    # ingest_hour) partitions present in this dataframe. Re-running an hour
    # is a no-op in effect, which is what makes reprocessing safe.
    #
    # No sortWithinPartitions: it forced a full sort of the hour in driver
    # memory for a marginal clustering gain. Iceberg's own write ordering
    # covers this if it is wanted, declared on the table rather than here.
    bronze.writeTo(target).overwritePartitions()


if __name__ == "__main__":
    sys.exit(main())
