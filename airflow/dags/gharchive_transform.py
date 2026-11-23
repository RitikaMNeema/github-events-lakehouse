"""Daily transform: bronze -> silver -> gold, with the gate in the middle.

Deliberately a separate DAG from ingest, on a separate schedule. Ingest
cadence is dictated by the source (hourly); transform cadence is dictated
by what consumers need (daily). Coupling them means a change to one
forces a change to the other.

Order matters here: the quality gate runs BEFORE dbt. A gate that runs
after the gold tables are built tells you that you have already published
bad numbers.
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

PROJECT = "/opt/project"
DAY_START = "{{ data_interval_start.strftime('%Y-%m-%dT00') }}"
DAY_END = "{{ data_interval_start.strftime('%Y-%m-%dT23') }}"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    dag_id="gharchive_transform",
    description="Daily silver + gold build with a blocking quality gate",
    default_args=default_args,
    schedule="0 2 * * *",   # 02:00 UTC, after the day's hours have landed
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,       # gold builds are not safe to run concurrently
    tags=["gharchive", "silver", "gold", "dbt"],
    doc_md=__doc__,
) as dag:

    # Do not transform a day whose hours have not all landed. Without this,
    # a partially-ingested day quietly produces a partial daily rollup that
    # looks exactly like a real one.
    wait_for_ingest = ExternalTaskSensor(
        task_id="wait_for_all_hours",
        external_dag_id="gharchive_ingest",
        external_task_id="raw_to_bronze",
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
        # The 24 hourly runs whose data interval falls inside this day.
        execution_date_fn=lambda dt: [dt.add(hours=h) for h in range(24)],
        mode="reschedule",   # free the worker slot while waiting
        poke_interval=300,
        timeout=60 * 60 * 6,
        doc_md="Blocks until all 24 hourly bronze loads for this day succeeded.",
    )

    silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f"cd {PROJECT} && python spark/jobs/silver_events.py "
            f"--start {DAY_START} --end {DAY_END}"
        ),
        doc_md=(
            "Normalises the legacy/v3 format break, types the payload per "
            "event type, quarantines invalid rows, and MERGEs on `event_id` "
            "so late-arriving events land in the correct event-time partition."
        ),
    )

    quality = BashOperator(
        task_id="quality_gate",
        bash_command=(
            f"cd {PROJECT} && python tests/quality_gate.py "
            f"--start {DAY_START} --end {DAY_END}"
        ),
        doc_md=(
            "Hard gate. Non-zero exit here stops the dbt build, so bad data "
            "cannot reach the gold tables consumers read."
        ),
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {PROJECT}/dbt && dbt deps --profiles-dir .",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {PROJECT}/dbt && dbt snapshot --profiles-dir .",
        doc_md=(
            "Advances the SCD2 repo dimension. Runs BEFORE the models, so "
            "today's facts join to a dimension that already knows today's "
            "repo names."
        ),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {PROJECT}/dbt && dbt build --profiles-dir . "
            "--vars '{lookback_days: 3}'"
        ),
        doc_md=(
            "`dbt build` runs each model and then its tests before any "
            "dependent model, so a failing test stops propagation instead of "
            "being reported after the fact."
        ),
    )

    compact = BashOperator(
        task_id="compact_tables",
        bash_command=(
            f"cd {PROJECT} && python spark/jobs/compaction.py "
            "--table silver.events && "
            f"cd {PROJECT} && python spark/jobs/compaction.py "
            "--table bronze.gh_events"
        ),
        # Maintenance must never fail the data pipeline: the data is already
        # correct, the files are just untidy.
        trigger_rule="all_done",
        retries=1,
        doc_md="Rewrites small files and expires snapshots older than 7 days.",
    )

    # Raw expiry, DRY RUN by default.
    #
    # Deliberately not armed. A DAG that deletes source data on a schedule
    # needs someone to have decided that on purpose, so this reports what it
    # WOULD remove and removes nothing. Add `--apply` to arm it, and only
    # once you have read docs/_measured/retention.md and are satisfied with
    # the refusal list.
    expire_raw = BashOperator(
        task_id="expire_raw_dry_run",
        bash_command=(
            f"cd {PROJECT} && python spark/jobs/expire_raw.py "
            f"--start {DAY_START} --end {DAY_END} --min-age-hours 168"
        ),
        trigger_rule="all_success",
        doc_md=(
            "Reports which raw objects are redundant because bronze provably "
            "holds the same information. Four gates: age, bronze coverage, "
            "silver coverage, and losslessness (no unmapped top-level keys). "
            "Dry-run: add --apply to actually reclaim the bytes."
        ),
    )

    throughput = BashOperator(
        task_id="throughput_ledger",
        bash_command=f"cd {PROJECT} && python tests/report_throughput.py",
        trigger_rule="all_done",
        doc_md=(
            "Bytes processed survives raw expiry in the manifest. This is the "
            "ledger behind any volume claim."
        ),
    )

    (
        wait_for_ingest
        >> silver
        >> quality
        >> dbt_deps
        >> dbt_snapshot
        >> dbt_build
        >> expire_raw
        >> compact
        >> throughput
    )
