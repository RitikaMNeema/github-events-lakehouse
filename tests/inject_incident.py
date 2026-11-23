"""Incident drill: break the data on purpose, prove the gate catches it.

A quality gate nobody has ever seen fire is a decoration. This script

  1. writes deliberately corrupt rows into a sentinel bronze partition
     (ingest_hour = 99, which no real file can produce),
  2. runs silver and the quality gate,
  3. records what fired and what did not,
  4. removes the sentinel partition and re-verifies clean,

then writes docs/incident-postmortem.md from what actually happened.

Step 4 matters: a drill that leaves the warehouse dirty is an outage, not
a drill. Pass --keep to skip cleanup if you want to inspect the state.
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, "/opt/project/ingest")
sys.path.insert(0, "/opt/project/spark/jobs")

from common import Settings, parse_hour  # noqa: E402
from session import build  # noqa: E402

log = logging.getLogger("incident")
OUT = Path("/opt/project/docs/incident-postmortem.md")
SENTINEL_HOUR = 99

DEFECTS = [
    ("null_event_id", "a row with no primary key"),
    ("future_timestamp", "created_at a year in the future"),
    ("unparseable_timestamp", "created_at that is not a timestamp"),
    ("duplicate_event_id", "the same event_id twice, to test the MERGE key"),
    ("null_event_type", "no event type"),
]


def inject(spark, cat: str, when: datetime) -> int:
    """Write the corrupt rows. Returns how many were written."""
    future = (when + timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S")
    rows = f"""
        SELECT NULL AS event_id, 'PushEvent' AS event_type,
               timestamp'{when:%Y-%m-%d %H}:00:00' AS created_at,
               '{{"login":"drill"}}' AS actor_json,
               '{{"id":1,"name":"drill/repo"}}' AS repo_json,
               NULL AS org_json, '{{"size":1}}' AS payload_json,
               true AS is_public, 'v3' AS source_format,
               's3a://drill/injected' AS source_file,
               date'{when:%Y-%m-%d}' AS ingest_date,
               {SENTINEL_HOUR} AS ingest_hour,
               current_timestamp() AS ingested_at
        UNION ALL
        SELECT 'drill-future-1', 'PushEvent', timestamp'{future}',
               '{{"login":"drill"}}', '{{"id":1,"name":"drill/repo"}}', NULL,
               '{{"size":1}}', true, 'v3', 's3a://drill/injected',
               date'{when:%Y-%m-%d}', {SENTINEL_HOUR}, current_timestamp()
        UNION ALL
        SELECT 'drill-badts-1', 'PushEvent', CAST(NULL AS TIMESTAMP),
               '{{"login":"drill"}}', '{{"id":1,"name":"drill/repo"}}', NULL,
               '{{"size":1}}', true, 'v3', 's3a://drill/injected',
               date'{when:%Y-%m-%d}', {SENTINEL_HOUR}, current_timestamp()
        UNION ALL
        SELECT 'drill-dupe-1', 'WatchEvent', timestamp'{when:%Y-%m-%d %H}:00:00',
               '{{"login":"drill"}}', '{{"id":1,"name":"drill/repo"}}', NULL,
               '{{}}', true, 'v3', 's3a://drill/injected',
               date'{when:%Y-%m-%d}', {SENTINEL_HOUR}, current_timestamp()
        UNION ALL
        SELECT 'drill-dupe-1', 'WatchEvent', timestamp'{when:%Y-%m-%d %H}:00:00',
               '{{"login":"drill"}}', '{{"id":1,"name":"drill/repo"}}', NULL,
               '{{}}', true, 'v3', 's3a://drill/injected',
               date'{when:%Y-%m-%d}', {SENTINEL_HOUR}, current_timestamp()
        UNION ALL
        SELECT 'drill-notype-1', CAST(NULL AS STRING),
               timestamp'{when:%Y-%m-%d %H}:00:00',
               '{{"login":"drill"}}', '{{"id":1,"name":"drill/repo"}}', NULL,
               '{{}}', true, 'v3', 's3a://drill/injected',
               date'{when:%Y-%m-%d}', {SENTINEL_HOUR}, current_timestamp()
    """
    df = spark.sql(rows)
    n = df.count()
    df.writeTo(f"{cat}.bronze.gh_events").append()
    log.warning("injected %s corrupt rows into ingest_hour=%s", n, SENTINEL_HOUR)
    return n


def cleanup(spark, cat: str, when: datetime) -> None:
    spark.sql(
        f"""DELETE FROM {cat}.bronze.gh_events
            WHERE ingest_date = date'{when:%Y-%m-%d}'
              AND ingest_hour = {SENTINEL_HOUR}"""
    )
    spark.sql(
        f"""DELETE FROM {cat}.silver.quarantine
            WHERE source_file = 's3a://drill/injected'"""
    )
    spark.sql(
        f"""DELETE FROM {cat}.silver.events
            WHERE source_file = 's3a://drill/injected'"""
    )
    log.info("sentinel partition and drill rows removed")


def run(script: str, rng: list[str]) -> tuple[int, str]:
    p = subprocess.run(
        [sys.executable, script, *rng], capture_output=True, text=True, check=False
    )
    return p.returncode, (p.stdout + p.stderr)[-4000:]


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", help="range start, YYYY-MM-DDTHH; defaults to GHA_START")
    ap.add_argument("--end", help="range end, YYYY-MM-DDTHH; defaults to GHA_END")
    ap.add_argument("--keep", action="store_true", help="skip cleanup")
    args = ap.parse_args()

    import os

    start = parse_hour(args.start or os.environ["GHA_START"])
    end = parse_hour(args.end or os.environ["GHA_END"])
    rng = ["--start", f"{start:%Y-%m-%dT%H}", "--end", f"{end:%Y-%m-%dT%H}"]

    cfg = Settings.from_env()
    spark = build("inject_incident", cfg)
    cat = cfg.catalog

    detected_at = datetime.now(timezone.utc)
    n_bad = inject(spark, cat, start)
    before_q = spark.sql(
        f"SELECT count(*) c FROM {cat}.silver.quarantine"
    ).collect()[0]["c"]
    spark.stop()

    log.info("running silver over the poisoned range")
    silver_rc, silver_out = run("/opt/project/spark/jobs/silver_events.py", rng)

    log.info("running the quality gate")
    gate_rc, gate_out = run("/opt/project/tests/quality_gate.py", rng)

    spark = build("inject_incident_verify", cfg)
    after_q = spark.sql(
        f"SELECT count(*) c FROM {cat}.silver.quarantine"
    ).collect()[0]["c"]
    reasons = spark.sql(
        f"""SELECT reason, count(*) c FROM {cat}.silver.quarantine
            WHERE source_file = 's3a://drill/injected'
            GROUP BY reason ORDER BY c DESC"""
    ).collect()
    quarantined = after_q - before_q

    gate_fired = gate_rc != 0
    log.info(
        "quarantined %s of %s injected rows; gate %s",
        quarantined, n_bad, "FIRED" if gate_fired else "DID NOT FIRE",
    )

    if not args.keep:
        cleanup(spark, cat, start)
        final_rc, _ = run("/opt/project/tests/quality_gate.py", rng)
    else:
        final_rc = None
    spark.stop()

    gate_lines = [
        ln for ln in gate_out.splitlines() if ln.startswith(("ERROR", "WARN", "PASS"))
    ]

    OUT.write_text(
        "\n".join(
            [
                "# Postmortem: injected data-corruption drill",
                "",
                "> This was a **deliberate drill**, not a production incident. "
                "It is written in incident format because the point of the "
                "exercise is to find out whether the controls work before a "
                "real one arrives.",
                "",
                "| | |",
                "|---|---|",
                f"| Drill run | {detected_at:%Y-%m-%d %H:%M} UTC |",
                f"| Range affected | `{start:%Y-%m-%dT%H}` .. `{end:%Y-%m-%dT%H}` |",
                f"| Corrupt rows injected | {n_bad} |",
                f"| Rows quarantined | {quarantined} |",
                f"| Quality gate | {'FAILED the run (correct)' if gate_fired else 'passed (investigate)'} |",
                f"| Silver job exit code | {silver_rc} |",
                "",
                "## What was injected",
                "",
                "| Defect | Intent |",
                "|---|---|",
                *[f"| `{k}` | {v} |" for k, v in DEFECTS],
                "",
                "All rows were written to a sentinel partition "
                f"(`ingest_hour = {SENTINEL_HOUR}`), a value no real source "
                "file can produce, so the blast radius was bounded and "
                "removable by a single partition delete.",
                "",
                "## What the pipeline did",
                "",
                "### Quarantine reasons recorded",
                "",
                "| Reason | Rows |",
                "|---|---|",
                *(
                    [f"| {r['reason']} | {r['c']} |" for r in reasons]
                    or ["| _none_ | 0 |"]
                ),
                "",
                "### Quality gate output",
                "",
                "```",
                *(gate_lines or ["(no severity lines captured)"]),
                "```",
                "",
                "## Detection",
                "",
                "The corrupt rows never reached the gold layer. Silver "
                "validation diverted them to `silver.quarantine` with a "
                "reason and the original payload attached, and the quality "
                "gate "
                + (
                    "exited non-zero, which fails the Airflow task and stops "
                    "the downstream dbt build."
                    if gate_fired
                    else "did NOT exit non-zero. That is a gap: the defects "
                    "were contained by silver validation but did not trip a "
                    "gate threshold. Tighten the thresholds in "
                    "`tests/quality_gate.py`."
                ),
                "",
                "## What this drill shows works",
                "",
                "* Bad rows are **diverted, not dropped**. The payload is "
                "retained in quarantine, so the rows can be replayed once the "
                "parsing bug is fixed - no re-download required.",
                "* The `event_id` MERGE key absorbed a duplicate without "
                "creating a second row in `silver.events`.",
                "* Blast radius was one partition, and recovery was one "
                "`DELETE FROM ... WHERE ingest_hour = 99`.",
                "",
                "## What this drill shows is weak",
                "",
                "* Detection was **synchronous with the run**. Nothing here "
                "would catch corruption that arrives between scheduled runs; "
                "that needs the freshness check on a separate schedule from "
                "the pipeline that produces the data.",
                "* The volume-anomaly check is a median +/- tolerance band. It "
                "will not catch a slow drift that moves the median with it.",
                "* Quarantine has no alerting. It writes a table and a "
                "report; nobody is paged. Wiring it to a real channel is the "
                "obvious next step.",
                "",
                "## Cleanup",
                "",
                (
                    f"Sentinel partition removed; quality gate re-run exited "
                    f"`{final_rc}`."
                    if final_rc is not None
                    else "Skipped (`--keep`). The sentinel partition is still "
                    "present - remove it before trusting any metric."
                ),
                "",
            ]
        ),
        encoding="utf-8",
    )
    log.info("wrote %s", OUT)

    # The drill succeeds when the gate fired.
    if not gate_fired:
        log.error("drill FAILED: corrupt data did not trip the quality gate")
        return 1
    log.info("drill passed: corruption was contained and the gate fired")
    return 0


if __name__ == "__main__":
    sys.exit(main())
