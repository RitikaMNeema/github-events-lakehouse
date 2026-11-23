"""End-to-end smoke test: Spark -> Iceberg -> S3-compatible storage.

Run this FIRST, before any pipeline job. It isolates "is the stack wired
correctly" from "is my pipeline logic correct", which otherwise fail in
indistinguishable ways: a missing jar and a bad SQL predicate both just
produce a stack trace.

Each step prints PASS/FAIL independently, so a failure names the layer:

  1. SparkSession    jars resolved, JVM up
  2. catalog         Iceberg JDBC catalog reachable; creates its own
                     metadata tables in Postgres on first contact
  3. namespace       catalog write path
  4. table + write   S3 write path (S3FileIO)
  5. read back       S3 read path, values intact
  6. metadata tables Iceberg system tables (.files, .snapshots) queryable
  7. s3a read        the Hadoop S3A path used for the raw zone
  8. cleanup         drop what we made

Exits non-zero on the first hard failure.
"""
from __future__ import annotations

import logging
import sys
import traceback

sys.path.insert(0, "/opt/project/ingest")
sys.path.insert(0, "/opt/project/spark/jobs")

from common import Settings  # noqa: E402

log = logging.getLogger("smoke")

SMOKE_NS = "smoke"
SMOKE_TBL = "smoke.probe"


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)-5s %(message)s")
    results: list[tuple[str, bool, str]] = []

    def step(name: str, fn):
        try:
            detail = fn() or ""
            results.append((name, True, str(detail)))
            log.info("PASS  %-18s %s", name, detail)
            return True
        except Exception as exc:  # noqa: BLE001
            results.append((name, False, f"{type(exc).__name__}: {exc}"))
            log.error("FAIL  %-18s %s", name, exc)
            log.debug(traceback.format_exc())
            return False

    cfg = Settings.from_env()
    cat = cfg.catalog
    log.info(
        "config: catalog=%s warehouse=%s endpoint=%s master=%s",
        cat, cfg.warehouse, cfg.s3_endpoint, cfg.spark_master,
    )

    # 1 -------------------------------------------------------------------
    spark = None

    def build_session():
        nonlocal spark
        from session import build

        spark = build("smoke_stack", cfg)
        return f"spark {spark.version}"

    if not step("SparkSession", build_session):
        return 1

    # 2 -------------------------------------------------------------------
    # Listing namespaces forces the JdbcCatalog to connect and, on first
    # contact, to CREATE its own metadata tables (iceberg_tables,
    # iceberg_namespace_properties). Trino cannot do this itself, which is
    # why running this smoke test is what makes Trino work afterwards.
    def catalog_reachable():
        rows = spark.sql(f"SHOW NAMESPACES IN {cat}").collect()
        return f"{len(rows)} namespace(s)"

    if not step("catalog", catalog_reachable):
        log.error("catalog unreachable - nothing downstream can work")
        return 1

    step("namespace", lambda: spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {cat}.{SMOKE_NS}") and "created")

    # 3 -------------------------------------------------------------------
    def write_table():
        spark.sql(f"DROP TABLE IF EXISTS {cat}.{SMOKE_TBL}")
        spark.sql(
            f"""CREATE TABLE {cat}.{SMOKE_TBL} (
                    id BIGINT, label STRING, ts TIMESTAMP
                ) USING iceberg
                PARTITIONED BY (days(ts))
                TBLPROPERTIES ('write.format.default'='parquet')"""
        )
        spark.sql(
            f"""INSERT INTO {cat}.{SMOKE_TBL} VALUES
                (1, 'alpha', timestamp'2023-06-15 00:00:00'),
                (2, 'beta',  timestamp'2023-06-15 01:00:00'),
                (3, 'gamma', timestamp'2023-06-16 02:00:00')"""
        )
        return "3 rows written"

    if not step("table + write", write_table):
        log.error("cannot write to object storage - check S3 endpoint/creds")
        return 1

    # 4 -------------------------------------------------------------------
    def read_back():
        rows = spark.sql(
            f"SELECT id, label FROM {cat}.{SMOKE_TBL} ORDER BY id"
        ).collect()
        got = [(r["id"], r["label"]) for r in rows]
        want = [(1, "alpha"), (2, "beta"), (3, "gamma")]
        if got != want:
            raise AssertionError(f"values differ: {got} != {want}")
        return "values intact"

    if not step("read back", read_back):
        return 1

    # 5 -------------------------------------------------------------------
    def metadata_tables():
        f = spark.sql(
            f"SELECT count(*) c, sum(record_count) r FROM {cat}.{SMOKE_TBL}.files"
        ).collect()[0]
        s = spark.sql(
            f"SELECT count(*) c FROM {cat}.{SMOKE_TBL}.snapshots"
        ).collect()[0]["c"]
        # 2 partitions (Jun 15 / Jun 16) => at least 2 data files
        if f["c"] < 2:
            raise AssertionError(f"expected >=2 data files, got {f['c']}")
        return f"{f['c']} files, {f['r']} records, {s} snapshot(s)"

    step("metadata tables", metadata_tables)

    # 6 -------------------------------------------------------------------
    # The raw zone is read through Hadoop S3A, a different code path from
    # Iceberg's S3FileIO. Both must work; only one is exercised above.
    def s3a_path():
        probe = f"s3a://{cfg.bucket}/smoke/s3a_probe.txt"
        spark.createDataFrame([("hello s3a",)], ["line"]).coalesce(1) \
            .write.mode("overwrite").text(probe)
        back = spark.read.text(probe).collect()
        if not back or back[0]["value"] != "hello s3a":
            raise AssertionError(f"round-trip mismatch: {back}")
        return "s3a round-trip ok"

    step("s3a read/write", s3a_path)

    # 7 -------------------------------------------------------------------
    def cleanup():
        spark.sql(f"DROP TABLE IF EXISTS {cat}.{SMOKE_TBL} PURGE")
        spark.sql(f"DROP NAMESPACE IF EXISTS {cat}.{SMOKE_NS}")
        return "dropped"

    step("cleanup", cleanup)

    if spark:
        spark.stop()

    # ---------------------------------------------------------------------
    failed = [n for n, ok, _ in results if not ok]
    print("\n" + "=" * 62)
    for name, ok, detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name:<18} {detail}")
    print("=" * 62)
    if failed:
        print(f"  {len(failed)} step(s) failed: {', '.join(failed)}")
        return 1
    print("  stack is wired correctly")
    return 0


if __name__ == "__main__":
    sys.exit(main())
