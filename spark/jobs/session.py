"""SparkSession wired to the Iceberg JDBC catalog over S3-compatible storage.

Two filesystem paths are configured on purpose:
  * Iceberg tables use S3FileIO  (s3://)  - the native, faster path.
  * Raw JSON reads use Hadoop S3A (s3a://) - because spark.read.text needs
    a Hadoop filesystem, and the raw zone is plain objects, not a table.
"""
from __future__ import annotations

import sys

from pyspark.sql import SparkSession

sys.path.insert(0, "/opt/project/ingest")
from common import Settings  # noqa: E402

ICEBERG_VERSION = "1.8.1"
HADOOP_AWS_VERSION = "3.3.4"  # must match the Hadoop that Spark 3.5 ships
PG_DRIVER_VERSION = "42.7.4"

PACKAGES = ",".join(
    [
        f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}",
        f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
        f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}",
        f"org.postgresql:postgresql:{PG_DRIVER_VERSION}",
    ]
)


def build(app_name: str, cfg: Settings | None = None) -> SparkSession:
    cfg = cfg or Settings.from_env()
    cat = cfg.catalog

    b = (
        SparkSession.builder.appName(app_name)
        .master(cfg.spark_master)
        .config("spark.jars.packages", PACKAGES)
        .config("spark.driver.memory", cfg.driver_memory)
        # More, smaller shuffle partitions: each task then holds less, and
        # spills to disk instead of blowing the heap. 32 made individual
        # tasks large enough to OOM a memory-capped driver.
        .config("spark.sql.shuffle.partitions", "64")
        # Let AQE coalesce them back down when the data is genuinely small,
        # so the higher count costs nothing on narrow ranges.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # ---- Iceberg ----
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            f"spark.sql.catalog.{cat}.catalog-impl",
            "org.apache.iceberg.jdbc.JdbcCatalog",
        )
        .config(
            f"spark.sql.catalog.{cat}.uri",
            f"jdbc:postgresql://{cfg.pg_host}:5432/iceberg",
        )
        .config(f"spark.sql.catalog.{cat}.jdbc.user", cfg.pg_user)
        .config(f"spark.sql.catalog.{cat}.jdbc.password", cfg.pg_password)
        .config(f"spark.sql.catalog.{cat}.warehouse", cfg.warehouse)
        .config(f"spark.sql.catalog.{cat}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{cat}.s3.endpoint", cfg.s3_endpoint)
        .config(f"spark.sql.catalog.{cat}.s3.path-style-access", "true")
        .config("spark.sql.defaultCatalog", cat)
        # ---- S3A, for reading the raw zone ----
        .config("spark.hadoop.fs.s3a.endpoint", cfg.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", cfg.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", cfg.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Non-AWS S3 implementations do not always reproduce AWS ETag
        # semantics, which S3A change detection assumes. Disabling it avoids
        # spurious "file changed during read" failures.
        .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
        .config("spark.hadoop.fs.s3a.change.detection.version.required", "false")
    )
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def raw_path(cfg: Settings, event_date: str, hour: str) -> str:
    return (
        f"s3a://{cfg.bucket}/raw/gharchive/"
        f"event_date={event_date}/hour={hour}/*.json.gz"
    )
