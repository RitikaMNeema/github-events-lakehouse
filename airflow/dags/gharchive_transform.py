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
# The calendar day to process is data_interval_start's date.
#
# With cron "0 2 * * *" the interval is [prev fire, this fire] = 02:00 -> 02:00
# and logical_date == data_interval_start, so the run that fires at 02:00 on
# 06-16 has data_interval_start 06-15 02:00 and processes calendar day 06-15:
# the day that finished two hours earlier. Correct.
#
# DO NOT validate this with `airflow dags test`. It assigns intervals that a
# scheduled run would never get -- asked for 2023-06-15 it supplied
# data_interval_start 2023-06-13 02:00, and the quality gate duly failed on a
# day that was never ingested. Use `airflow dags backfill` for anything
# date-sensitive; it applies the real timetable.
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
    start_date=pendulum.datetime(2023, 6, 15, tz="UTC"),
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
        #
        # range(24): for a scheduled or backfilled run, Airflow sets
        # data_interval_start EQUAL to the logical date, so the run dated
        # 09:00 processes hour 09. Verified from backfill logs -- logical
        # 01:00 ran `--start 2023-06-15T01`, logical 23:00 ran
        # `--start 2023-06-15T23`.
        #
        # Beware `airflow dags test`: it assigns a DIFFERENT interval for the
        # same date (logical 08:00 got data_interval_start 07:00), which looks
        # exactly like an off-by-one in this DAG and is not. Do not "fix" this
        # line on the strength of a `dags test` run, and do not mix
        # `dags test` with a backfill over the same dates -- a DagRun created
        # by the former keeps its interval through --reset-dagruns, so the
        # backfill silently reprocesses the wrong hour and leaves a gap.
        # start_of("day"), not dt directly.
        #
        # This DAG's schedule is "0 2 * * *", so its logical date is 02:00 --
        # not midnight. Using dt as the anchor would look for ingest runs at
        # 02:00..01:00-next-day: missing hours 00 and 01 of the very day it
        # guards, while demanding two hours of the following day that have no
        # business blocking it.
        #
        # Anchoring to the start of the day makes the sensor independent of
        # whatever hour this DAG happens to be scheduled at.
        execution_date_fn=lambda dt: [
            dt.start_of("day").add(hours=h) for h in range(24)
        ],
        # poke, NOT reschedule.
        #
        # reschedule is the better production default -- it frees the worker
        # slot between pokes -- but a reschedule-mode sensor as a DAG's first
        # task DEAD-LOCKS `airflow dags backfill`:
        #
        #   ERROR - Backfill job dead-locked.
        #
        # The backfill job cannot make progress while the sensor defers. For a
        # project whose central claim is that backfill and steady state run
        # the SAME code path, a gate that deadlocks backfill is not a gate.
        # So: hold the slot. With a 60s poke and a 1h timeout that is at most
        # one occupied slot for an hour, which this DAG can afford.
        mode="poke",
        poke_interval=60,
        # 1 hour, not 6. This DAG runs at 02:00 on a day whose hours all
        # landed by 00:59. If they are not all there within an hour,
        # something needs a human, and six hours of silent poking only
        # delays finding that out.
        timeout=60 * 60,
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
        # all_success, NOT all_done.
        #
        # all_done ran this task even when the completeness sensor had FAILED,
        # and because this and throughput_ledger are the DAG's leaf tasks,
        # their success marked the whole DagRun green -- while silver, the
        # quality gate and dbt had never run at all. A pipeline that reports
        # success after its gate failed is worse than no pipeline: it lies to
        # whatever is watching it.
        #
        # The original intent was "a compaction failure should not fail the
        # data pipeline". That is a statement about THIS task failing, not a
        # licence to run after upstream failed. Retries cover the former.
        trigger_rule="all_success",
        retries=2,
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
        # all_success for the same reason as compact_tables: a throughput
        # report written after a failed gate documents a run that did not
        # happen.
        trigger_rule="all_success",
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
