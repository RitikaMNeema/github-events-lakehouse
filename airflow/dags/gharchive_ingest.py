"""Hourly ingest: GH Archive -> raw zone -> Iceberg bronze.

This DAG *is* the backfill mechanism. Because every task takes its hour
from the data interval and every step is idempotent, backfilling a year is

    airflow dags backfill --start-date ... --end-date ... gharchive_ingest

with no separate code path. A pipeline with a different code path for
backfill than for steady state has two pipelines, and one of them is
always broken.
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

HOUR = "{{ data_interval_start.strftime('%Y-%m-%dT%H') }}"
PROJECT = "/opt/project"

default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": pendulum.duration(minutes=30),
}

with DAG(
    dag_id="gharchive_ingest",
    description="Hourly GH Archive ingest into the bronze layer",
    default_args=default_args,
    schedule="@hourly",
    # GH Archive publishes from 2011-02-12. Start where you have disk for.
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=True,
    # Bound the concurrency: a wide backfill must not exhaust the laptop or
    # hammer the source. Four hours in flight is a deliberate throttle.
    max_active_runs=4,
    # The source publishes an hour's file shortly after the hour ends; if we
    # have not landed it within three hours, something is wrong.
    dagrun_timeout=pendulum.duration(hours=2),
    tags=["gharchive", "ingest", "bronze"],
    doc_md=__doc__,
) as dag:

    fetch = BashOperator(
        task_id="fetch_raw",
        bash_command=(
            f"cd {PROJECT} && python ingest/fetch_gharchive.py "
            f"--start {HOUR} --end {HOUR}"
        ),
        doc_md=(
            "Streams the hour's `.json.gz` straight into object storage. "
            "Records the outcome in `ingest_manifest`. A 404 is recorded as "
            "`missing_upstream` and does NOT fail the task, because upstream "
            "gaps are a known property of the source."
        ),
        # The SLA that matters to consumers: raw landed within the hour.
        sla=pendulum.duration(hours=1),
    )

    bronze = BashOperator(
        task_id="raw_to_bronze",
        bash_command=(
            f"cd {PROJECT} && python spark/jobs/bronze_raw_to_iceberg.py "
            f"--start {HOUR} --end {HOUR}"
        ),
        doc_md=(
            "Dynamic partition overwrite on `(ingest_date, ingest_hour)`. "
            "Safe to re-run: the task replaces exactly this hour's partition."
        ),
    )

    fetch >> bronze
