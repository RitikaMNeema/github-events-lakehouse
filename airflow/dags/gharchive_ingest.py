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

# data_interval_START, not the logical date.
#
# For an @hourly DAG the run at logical date 06:00 covers the interval
# 05:00 -> 06:00, so it processes hour 05: the hour that has just CLOSED and
# is therefore complete at the source. Using the logical date instead would
# reach for an hour the source has not finished publishing.
#
# Worth stating plainly because the off-by-one bites every time: a run
# labelled 06:00 writes ingest_hour=05.
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
    # 2023, not 2026. The recent GH Archive feed is degraded: a measured
    # 2026-11-01 hour carried 4,720 events and no PushEvent at all, against
    # 198,897 events and 62% PushEvent for 2023-06-15. Scheduling from a
    # recent date would backfill hundreds of near-empty hours.
    start_date=pendulum.datetime(2023, 6, 15, tz="UTC"),
    catchup=True,
    # ONE run at a time. This is not conservatism, it is a hard constraint.
    #
    # Spark runs in local mode INSIDE the scheduler container, so every
    # concurrent DAG run means another 2 GB Spark driver in the same 3 GB
    # cgroup. Four concurrent runs would be ~8 GB of driver heap in a 3 GB
    # container: guaranteed OOM-kill, which is exactly how this project
    # already lost two runs before the cause was understood.
    #
    # Raising this is only safe once Spark submits to a cluster (or its own
    # container) rather than running in-process. The limit belongs with the
    # execution model, not with taste.
    max_active_runs=1,
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
