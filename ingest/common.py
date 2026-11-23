"""Shared config, S3 and manifest helpers.

Deliberately dependency-light: os.environ for config, boto3 for object
storage, psycopg2 for the manifest. No framework magic, so every job can
also be run by hand from `make shell`.
"""
from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import boto3
import psycopg2
from botocore.client import Config

GHA_BASE = "https://data.gharchive.org"
HOUR_FMT = "%Y-%m-%dT%H"


# ----------------------------------------------------------------- config
@dataclass(frozen=True)
class Settings:
    bucket: str
    s3_endpoint: str
    access_key: str
    secret_key: str
    region: str
    pg_host: str
    pg_user: str
    pg_password: str
    catalog: str
    warehouse: str
    spark_master: str
    driver_memory: str

    @staticmethod
    def from_env() -> "Settings":
        return Settings(
            bucket=os.environ.get("LAKE_BUCKET", "lakehouse"),
            s3_endpoint=os.environ.get("S3_ENDPOINT", "http://minio:9000"),
            access_key=os.environ["AWS_ACCESS_KEY_ID"],
            secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            region=os.environ.get("AWS_REGION", "us-east-1"),
            pg_host=os.environ.get("PG_HOST", "postgres"),
            pg_user=os.environ.get("POSTGRES_USER", "lake"),
            pg_password=os.environ.get("POSTGRES_PASSWORD", "lake"),
            catalog=os.environ.get("ICEBERG_CATALOG", "lake"),
            warehouse=os.environ.get("ICEBERG_WAREHOUSE", "s3://lakehouse/warehouse"),
            spark_master=os.environ.get("SPARK_MASTER", "local[*]"),
            driver_memory=os.environ.get("SPARK_DRIVER_MEMORY", "4g"),
        )


def s3_client(cfg: Settings):
    return boto3.client(
        "s3",
        endpoint_url=cfg.s3_endpoint,
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
        region_name=cfg.region,
        config=Config(s3={"addressing_style": "path"}),
    )


def pg_conn(cfg: Settings, dbname: str = "airflow"):
    return psycopg2.connect(
        host=cfg.pg_host, dbname=dbname, user=cfg.pg_user, password=cfg.pg_password
    )


# ------------------------------------------------------------ hour ranges
def parse_hour(s: str) -> datetime:
    return datetime.strptime(s, HOUR_FMT).replace(tzinfo=timezone.utc)


def hour_range(start: datetime, end: datetime):
    """Inclusive range of UTC hours."""
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(hours=1)


def gha_url(hour: datetime) -> str:
    # GH Archive does NOT zero-pad the hour: 2026-11-01-7.json.gz
    return f"{GHA_BASE}/{hour:%Y-%m-%d}-{hour.hour}.json.gz"


def raw_key(hour: datetime) -> str:
    """Hive-style layout so Spark gets partition pruning for free."""
    return (
        f"raw/gharchive/event_date={hour:%Y-%m-%d}/hour={hour:%H}/"
        f"{hour:%Y-%m-%d}-{hour.hour}.json.gz"
    )


def add_range_args(p: argparse.ArgumentParser) -> argparse.ArgumentParser:
    p.add_argument("--start", help=f"inclusive start hour, {HOUR_FMT}")
    p.add_argument("--end", help=f"inclusive end hour, {HOUR_FMT}")
    p.add_argument(
        "--from-env",
        action="store_true",
        help="read GHA_START/GHA_END from the environment",
    )
    return p


def resolve_range(args) -> tuple[datetime, datetime]:
    if args.from_env or not (args.start and args.end):
        start = os.environ.get("GHA_START")
        end = os.environ.get("GHA_END")
        if not (start and end):
            raise SystemExit("set --start/--end or GHA_START/GHA_END in .env")
        return parse_hour(start), parse_hour(end)
    return parse_hour(args.start), parse_hour(args.end)


# --------------------------------------------------------------- manifest
MANIFEST_DDL = """
CREATE TABLE IF NOT EXISTS ingest_manifest (
    hour_ts      timestamptz NOT NULL,
    source_url   text        NOT NULL,
    object_key   text,
    size_bytes   bigint,
    etag         text,
    status       text        NOT NULL,
    attempts     int         NOT NULL DEFAULT 1,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (hour_ts)
);
COMMENT ON TABLE ingest_manifest IS
  'One row per source hour. status: fetched | present | missing_upstream | failed';
"""


def ensure_manifest(cfg: Settings) -> None:
    with pg_conn(cfg) as c, c.cursor() as cur:
        cur.execute(MANIFEST_DDL)


def record(cfg: Settings, hour, url, key, size, etag, status) -> None:
    with pg_conn(cfg) as c, c.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingest_manifest
                (hour_ts, source_url, object_key, size_bytes, etag, status)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (hour_ts) DO UPDATE SET
                object_key = EXCLUDED.object_key,
                size_bytes = EXCLUDED.size_bytes,
                etag       = EXCLUDED.etag,
                status     = EXCLUDED.status,
                attempts   = ingest_manifest.attempts + 1,
                updated_at = now();
            """,
            (hour, url, key, size, etag, status),
        )
