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

from pyspark.sql import functions as F

sys.path.insert(0, "/opt/project/ingest")
sys.path.insert(0, "/opt/project/spark/jobs")

from common import Settings, add_range_args, hour_range, resolve_range  # noqa: E402
from session import build, raw_path  # noqa: E402

log = logging.getLogger("bronze")

TABLE = "bronze.gh_events"

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

    hours = list(hour_range(start, end))
    paths = [raw_path(cfg, f"{h:%Y-%m-%d}", f"{h:%H}") for h in hours]

    # Missing hours are expected (upstream gaps). Let Spark skip them rather
    # than failing the batch.
    df_raw = (
        spark.read.option("recursiveFileLookup", "false")
        .text(paths)
        .withColumn("source_file", F.input_file_name())
    )

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

    target = f"{cfg.catalog}.{TABLE}"
    # Dynamic partition overwrite: replaces only the (ingest_date, ingest_hour)
    # partitions present in this dataframe. Re-running is a no-op in effect.
    bronze.sortWithinPartitions("event_type").writeTo(target).overwritePartitions()

    n = spark.sql(
        f"""SELECT count(*) c FROM {target}
            WHERE ingest_date BETWEEN date'{start:%Y-%m-%d}' AND date'{end:%Y-%m-%d}'"""
    ).collect()[0]["c"]
    log.info("bronze now holds %s rows for the requested range", f"{n:,}")

    snap = spark.sql(
        f"SELECT snapshot_id, operation, summary FROM {target}.snapshots "
        "ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    if snap:
        log.info("latest snapshot %s (%s)", snap[0]["snapshot_id"], snap[0]["operation"])

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
