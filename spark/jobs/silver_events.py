"""Bronze -> silver: normalise the format break, type the payload, quarantine junk.

Three jobs in one pass:

  silver.events      one typed row per event, partitioned by EVENT date.
                     Written with MERGE on event_id, so an event that
                     arrives in a later file still lands in its correct
                     event-time partition exactly once.

  silver.commits     payload.commits exploded out of PushEvent. NOTE the
                     source truncates this array (~20); commit_count on
                     silver.events carries the true total from payload.size.
                     Anyone counting rows here is counting a sample.

  silver.quarantine  rows that failed validation, WITH the reason and the
                     original payload. Dropping bad rows silently is how you
                     get a dashboard that is quietly wrong for a year.
"""
from __future__ import annotations

import argparse
import logging
import sys

from pyspark.sql import DataFrame, functions as F

sys.path.insert(0, "/opt/project/ingest")
sys.path.insert(0, "/opt/project/spark/jobs")

from common import (  # noqa: E402
    Settings,
    add_range_args,
    hour_range,
    resolve_range,
)
from session import build  # noqa: E402

log = logging.getLogger("silver")

EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS {cat}.silver.events (
    event_id        STRING  NOT NULL,
    event_type      STRING  NOT NULL,
    event_ts        TIMESTAMP NOT NULL,
    actor_id        BIGINT,
    actor_login     STRING,
    repo_id         BIGINT,
    repo_name       STRING  COMMENT 'owner/name, normalised across both source formats',
    org_login       STRING,
    is_public       BOOLEAN,
    action          STRING  COMMENT 'opened/closed/created/... where the type has one',
    ref             STRING,
    ref_type        STRING,
    pr_number       INT,
    pr_merged       BOOLEAN,
    pr_additions    INT,
    pr_deletions    INT,
    pr_changed_files INT,
    issue_number    INT,
    comment_id      BIGINT,
    commit_count    INT     COMMENT 'payload.size: TRUE push size, not the truncated array',
    release_tag     STRING,
    source_format   STRING,
    source_file     STRING,
    processed_at    TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(event_ts))
TBLPROPERTIES (
    'write.format.default'            = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes'    = '134217728',
    'write.distribution-mode'         = 'hash',
    'format-version'                  = '2'
)
"""

COMMITS_DDL = """
CREATE TABLE IF NOT EXISTS {cat}.silver.commits (
    event_id     STRING NOT NULL,
    event_ts     TIMESTAMP NOT NULL,
    repo_id      BIGINT,
    repo_name    STRING,
    actor_login  STRING,
    commit_sha   STRING,
    author_name  STRING,
    author_email STRING,
    message      STRING,
    is_distinct  BOOLEAN,
    processed_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(event_ts))
TBLPROPERTIES ('write.format.default'='parquet',
               'write.parquet.compression-codec'='zstd',
               'format-version'='2')
"""

QUARANTINE_DDL = """
CREATE TABLE IF NOT EXISTS {cat}.silver.quarantine (
    event_id      STRING,
    event_type    STRING,
    reason        STRING COMMENT 'why this row was rejected',
    ingest_date   DATE,
    ingest_hour   INT,
    source_file   STRING,
    raw_payload   STRING COMMENT 'kept so the row can be replayed after a fix',
    quarantined_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (ingest_date)
TBLPROPERTIES ('write.format.default'='parquet', 'format-version'='2')
"""


def normalise(bronze: DataFrame) -> DataFrame:
    a, r, o, p = (
        F.col("actor_json"),
        F.col("repo_json"),
        F.col("org_json"),
        F.col("payload_json"),
    )
    is_v3 = F.col("source_format") == "v3"

    # Legacy `actor` is a bare login string; v3 is an object. get_json_object
    # returns a scalar unquoted, so coalesce handles both with no branching.
    actor_login = F.coalesce(F.get_json_object(a, "$.login"), a)

    # Legacy: {"name":"repo","owner":"owner"}. v3: {"name":"owner/repo"}.
    repo_name = F.when(is_v3, F.get_json_object(r, "$.name")).otherwise(
        F.concat_ws("/", F.get_json_object(r, "$.owner"), F.get_json_object(r, "$.name"))
    )

    return bronze.select(
        "event_id",
        "event_type",
        F.col("created_at").alias("event_ts"),
        F.get_json_object(a, "$.id").cast("bigint").alias("actor_id"),
        actor_login.alias("actor_login"),
        F.get_json_object(r, "$.id").cast("bigint").alias("repo_id"),
        repo_name.alias("repo_name"),
        F.coalesce(
            F.get_json_object(o, "$.login"), F.get_json_object(r, "$.organization")
        ).alias("org_login"),
        "is_public",
        F.get_json_object(p, "$.action").alias("action"),
        F.get_json_object(p, "$.ref").alias("ref"),
        F.get_json_object(p, "$.ref_type").alias("ref_type"),
        F.coalesce(
            F.get_json_object(p, "$.pull_request.number"),
            F.get_json_object(p, "$.number"),
        )
        .cast("int")
        .alias("pr_number"),
        (F.get_json_object(p, "$.pull_request.merged") == "true").alias("pr_merged"),
        F.get_json_object(p, "$.pull_request.additions").cast("int").alias("pr_additions"),
        F.get_json_object(p, "$.pull_request.deletions").cast("int").alias("pr_deletions"),
        F.get_json_object(p, "$.pull_request.changed_files")
        .cast("int")
        .alias("pr_changed_files"),
        F.get_json_object(p, "$.issue.number").cast("int").alias("issue_number"),
        F.get_json_object(p, "$.comment.id").cast("bigint").alias("comment_id"),
        F.get_json_object(p, "$.size").cast("int").alias("commit_count"),
        F.get_json_object(p, "$.release.tag_name").alias("release_tag"),
        "source_format",
        "source_file",
        F.current_timestamp().alias("processed_at"),
    )


def _process_hour(spark, cat: str, hour) -> None:
    """Validate, normalise and merge ONE ingest hour into silver.

    Scoped to a single hour on purpose. The whole-range version made peak
    memory grow with how wide the request was, and six hours (~2.8 GB of
    uncompressed JSON) killed a 2 GB driver outright -- the JVM died, not
    the query ("Answer from Java side is empty"). A job that needs more RAM
    to cover more time cannot backfill a year, which is the point of it.

    Per-hour also means each hour is its own set of commits, so a failure
    part way through a backfill leaves every completed hour intact.
    """
    bronze = spark.table(f"{cat}.bronze.gh_events").where(
        f"ingest_date = date'{hour:%Y-%m-%d}' AND ingest_hour = {hour.hour}"
    )
    log.info("  reading %s bronze rows", f"{bronze.count():,}")

    # ---------------------------------------------------------- validation
    reason = (
        F.when(F.col("event_id").isNull(), F.lit("null event_id"))
        .when(F.col("created_at").isNull(), F.lit("unparseable created_at"))
        .when(F.col("event_type").isNull(), F.lit("null event_type"))
        .when(
            F.col("created_at") < F.lit("2011-02-12").cast("timestamp"),
            F.lit("event_ts before GH Archive existed"),
        )
        .when(
            F.col("created_at") > F.current_timestamp() + F.expr("INTERVAL 1 DAY"),
            F.lit("event_ts in the future"),
        )
        .otherwise(F.lit(None))
    )
    tagged = bronze.withColumn("_reason", reason)
    bad = tagged.where(F.col("_reason").isNotNull())
    good = tagged.where(F.col("_reason").isNull()).drop("_reason")

    n_bad = bad.count()
    if n_bad:
        log.warning("quarantining %s rows", f"{n_bad:,}")
        bad.select(
            "event_id",
            "event_type",
            F.col("_reason").alias("reason"),
            "ingest_date",
            "ingest_hour",
            "source_file",
            F.col("payload_json").alias("raw_payload"),
            F.current_timestamp().alias("quarantined_at"),
        ).writeTo(f"{cat}.silver.quarantine").append()

    # ------------------------------------------------------------- events
    src = normalise(good).dropDuplicates(["event_id"])
    src.createOrReplaceTempView("silver_src")

    cols = [c for c in src.columns]
    set_clause = ", ".join(f"t.{c} = s.{c}" for c in cols if c != "event_id")
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join(f"s.{c}" for c in cols)

    spark.sql(
        f"""
        MERGE INTO {cat}.silver.events t
        USING silver_src s
          ON t.event_id = s.event_id
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
    )
    log.info("  merged events")

    # ------------------------------------------------------------ commits
    # `distinct` is a reserved word in Spark's DDL parser, hence the backticks.
    commit_schema = (
        "array<struct<"
        "sha:string, message:string, `distinct`:boolean, "
        "author:struct<name:string, email:string>"
        ">>"
    )
    pushes = good.where(F.col("event_type") == "PushEvent").select(
        "event_id",
        F.col("created_at").alias("event_ts"),
        F.get_json_object(F.col("repo_json"), "$.id").cast("bigint").alias("repo_id"),
        F.coalesce(
            F.get_json_object(F.col("repo_json"), "$.name"),
            F.concat_ws(
                "/",
                F.get_json_object(F.col("repo_json"), "$.owner"),
                F.get_json_object(F.col("repo_json"), "$.name"),
            ),
        ).alias("repo_name"),
        F.coalesce(
            F.get_json_object(F.col("actor_json"), "$.login"), F.col("actor_json")
        ).alias("actor_login"),
        F.from_json(
            F.get_json_object(F.col("payload_json"), "$.commits"), commit_schema
        ).alias("commits"),
    )
    exploded = (
        pushes.where(F.col("commits").isNotNull())
        .select("event_id", "event_ts", "repo_id", "repo_name", "actor_login",
                F.explode("commits").alias("c"))
        .select(
            "event_id", "event_ts", "repo_id", "repo_name", "actor_login",
            F.col("c.sha").alias("commit_sha"),
            F.col("c.author.name").alias("author_name"),
            F.col("c.author.email").alias("author_email"),
            F.col("c.message").alias("message"),
            F.col("c.`distinct`").alias("is_distinct"),
            F.current_timestamp().alias("processed_at"),
        )
        .dropDuplicates(["event_id", "commit_sha"])
    )
    exploded.createOrReplaceTempView("commits_src")
    spark.sql(
        f"""
        MERGE INTO {cat}.silver.commits t
        USING commits_src s
          ON t.event_id = s.event_id AND t.commit_sha = s.commit_sha
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    log.info("  merged commits")



def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = add_range_args(argparse.ArgumentParser(description=__doc__))
    args = ap.parse_args()

    cfg = Settings.from_env()
    start, end = resolve_range(args)
    cat = cfg.catalog
    spark = build("silver_events", cfg)

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.silver")
    for ddl in (EVENTS_DDL, COMMITS_DDL, QUARANTINE_DDL):
        spark.sql(ddl.format(cat=cat))

    # One hour per pass; see _process_hour for why.
    hours = list(hour_range(start, end))
    log.info("processing %d hour(s), one at a time", len(hours))
    for n, h in enumerate(hours, 1):
        log.info("[%d/%d] %s", n, len(hours), f"{h:%Y-%m-%dT%H}")
        _process_hour(spark, cat, h)
        # Drop cached plans/temp views between hours. Without this, driver
        # state accumulates across iterations and a long backfill dies
        # part way through for reasons unrelated to any single hour.
        spark.catalog.clearCache()

    for t in ("events", "commits", "quarantine"):
        c = spark.sql(f"SELECT count(*) c FROM {cat}.silver.{t}").collect()[0]["c"]
        log.info("silver.%-10s %12s rows", t, f"{c:,}")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
