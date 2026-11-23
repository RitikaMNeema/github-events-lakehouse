"""Quality gate. Exits non-zero on an ERROR so Airflow fails the run.

Every check answers a question a consumer would actually ask, and every
one distinguishes "the source had a hole" from "my pipeline broke". That
distinction is the whole job: a gate that cannot tell them apart gets
muted within a week, and then it is worse than nothing.

Severities:
  ERROR  the data is wrong or unusable -> fail the DAG, page someone
  WARN   worth a look, not worth waking up for
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, "/opt/project/ingest")
sys.path.insert(0, "/opt/project/spark/jobs")

from common import Settings, add_range_args, hour_range, pg_conn, resolve_range  # noqa: E402
from session import build  # noqa: E402

log = logging.getLogger("quality")
OUT = Path("/opt/project/docs/_measured/quality_report.md")

MAX_QUARANTINE_RATE = 0.005   # 0.5%
FRESHNESS_HOURS = 24
VOLUME_TOLERANCE = 0.4        # +/- 40% of the median hour


class Results:
    def __init__(self) -> None:
        self.rows: list[tuple[str, str, str]] = []

    def add(self, severity: str, check: str, detail: str) -> None:
        self.rows.append((severity, check, detail))
        getattr(log, {"ERROR": "error", "WARN": "warning", "PASS": "info"}[severity])(
            "%-5s %s: %s", severity, check, detail
        )

    @property
    def failed(self) -> bool:
        return any(s == "ERROR" for s, _, _ in self.rows)


def check_manifest(cfg, start, end, r: Results) -> None:
    expected = {h.replace(tzinfo=timezone.utc) for h in hour_range(start, end)}
    with pg_conn(cfg) as c, c.cursor() as cur:
        cur.execute(
            "SELECT hour_ts, status FROM ingest_manifest "
            "WHERE hour_ts BETWEEN %s AND %s",
            (start, end),
        )
        seen = {row[0]: row[1] for row in cur.fetchall()}

    missing_rows = expected - set(seen)
    failed = [h for h, s in seen.items() if s == "failed"]
    gaps = [h for h, s in seen.items() if s == "missing_upstream"]

    if missing_rows:
        r.add("ERROR", "manifest completeness",
              f"{len(missing_rows)} hour(s) never attempted, e.g. "
              f"{sorted(missing_rows)[0]:%Y-%m-%dT%H}")
    elif failed:
        r.add("ERROR", "manifest completeness",
              f"{len(failed)} hour(s) failed to fetch: "
              f"{', '.join(f'{h:%m-%dT%H}' for h in sorted(failed)[:5])}")
    else:
        r.add("PASS", "manifest completeness",
              f"all {len(expected)} hour(s) accounted for")

    # Upstream gaps are a property of the source, not a defect in the
    # pipeline. Reported, never fatal.
    if gaps:
        r.add("WARN", "upstream gaps",
              f"{len(gaps)} hour(s) absent at source (known GH Archive "
              f"behaviour): {', '.join(f'{h:%m-%dT%H}' for h in sorted(gaps)[:5])}")


def check_uniqueness(spark, cat, start, end, r: Results) -> None:
    dup = spark.sql(
        f"""
        SELECT count(*) AS c FROM (
            SELECT event_id FROM {cat}.silver.events
            WHERE event_ts BETWEEN timestamp'{start:%Y-%m-%d %H}:00:00'
                               AND timestamp'{end:%Y-%m-%d %H}:59:59'
            GROUP BY event_id HAVING count(*) > 1
        )
        """
    ).collect()[0]["c"]
    if dup:
        r.add("ERROR", "event_id uniqueness",
              f"{dup:,} duplicated event_id(s) - the MERGE key is not holding")
    else:
        r.add("PASS", "event_id uniqueness", "no duplicates")


def check_quarantine(spark, cat, start, end, r: Results) -> None:
    good = spark.sql(
        f"""SELECT count(*) c FROM {cat}.silver.events
            WHERE event_ts BETWEEN timestamp'{start:%Y-%m-%d %H}:00:00'
                               AND timestamp'{end:%Y-%m-%d %H}:59:59'"""
    ).collect()[0]["c"]
    bad = spark.sql(
        f"""SELECT count(*) c FROM {cat}.silver.quarantine
            WHERE ingest_date BETWEEN date'{start:%Y-%m-%d}' AND date'{end:%Y-%m-%d}'"""
    ).collect()[0]["c"]
    total = good + bad
    rate = (bad / total) if total else 0.0
    detail = f"{bad:,} quarantined of {total:,} ({rate:.3%})"
    if rate > MAX_QUARANTINE_RATE:
        r.add("ERROR", "quarantine rate", f"{detail}, threshold {MAX_QUARANTINE_RATE:.2%}")
    else:
        r.add("PASS", "quarantine rate", detail)


def check_volume(spark, cat, start, end, r: Results) -> None:
    """Volume anomaly: an hour far off the median means a truncated load."""
    rows = spark.sql(
        f"""
        SELECT ingest_date, ingest_hour, count(*) AS c
        FROM {cat}.bronze.gh_events
        WHERE ingest_date BETWEEN date'{start:%Y-%m-%d}' AND date'{end:%Y-%m-%d}'
        GROUP BY ingest_date, ingest_hour
        ORDER BY ingest_date, ingest_hour
        """
    ).collect()
    if len(rows) < 3:
        r.add("WARN", "volume anomaly", "too few hours loaded to judge")
        return
    counts = sorted(x["c"] for x in rows)
    median = counts[len(counts) // 2]
    lo, hi = median * (1 - VOLUME_TOLERANCE), median * (1 + VOLUME_TOLERANCE)
    outliers = [x for x in rows if not (lo <= x["c"] <= hi)]
    if outliers:
        worst = min(outliers, key=lambda x: x["c"])
        r.add("WARN", "volume anomaly",
              f"{len(outliers)} of {len(rows)} hour(s) outside "
              f"+/-{VOLUME_TOLERANCE:.0%} of median {median:,}; lowest "
              f"{worst['ingest_date']}T{worst['ingest_hour']:02d} at {worst['c']:,}")
    else:
        r.add("PASS", "volume anomaly", f"all hours within tolerance of median {median:,}")


def check_freshness(spark, cat, r: Results) -> None:
    row = spark.sql(f"SELECT max(processed_at) m FROM {cat}.silver.events").collect()[0]
    if row["m"] is None:
        r.add("ERROR", "freshness", "silver.events is empty")
        return
    age = (datetime.now(timezone.utc) - row["m"].replace(tzinfo=timezone.utc))
    hours = age.total_seconds() / 3600
    detail = f"last processed {hours:.1f}h ago"
    if hours > FRESHNESS_HOURS:
        r.add("ERROR", "freshness", f"{detail}, SLA {FRESHNESS_HOURS}h")
    else:
        r.add("PASS", "freshness", detail)


def check_referential(spark, cat, r: Results) -> None:
    orphans = spark.sql(
        f"""SELECT count(*) c FROM {cat}.silver.events
            WHERE repo_id IS NULL OR actor_login IS NULL"""
    ).collect()[0]["c"]
    if orphans:
        r.add("WARN", "dimension keys",
              f"{orphans:,} event(s) with a null repo_id/actor_login "
              "(filtered out by stg_events)")
    else:
        r.add("PASS", "dimension keys", "no null dimension keys")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = add_range_args(argparse.ArgumentParser(description=__doc__)).parse_args()
    cfg = Settings.from_env()
    start, end = resolve_range(args)
    spark = build("quality_gate", cfg)
    cat = cfg.catalog
    r = Results()

    check_manifest(cfg, start, end, r)
    check_volume(spark, cat, start, end, r)
    check_uniqueness(spark, cat, start, end, r)
    check_quarantine(spark, cat, start, end, r)
    check_referential(spark, cat, r)
    check_freshness(spark, cat, r)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(
        "\n".join(
            [
                "# Quality gate report",
                "",
                f"Run {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC over "
                f"`{start:%Y-%m-%dT%H}` .. `{end:%Y-%m-%dT%H}` UTC.",
                "",
                "| Severity | Check | Detail |",
                "|---|---|---|",
                *[f"| {s} | {c} | {d} |" for s, c, d in r.rows],
                "",
                f"**Result: {'FAILED' if r.failed else 'PASSED'}**",
                "",
            ]
        ),
        encoding="utf-8",
    )
    log.info("wrote %s", OUT)
    spark.stop()
    return 1 if r.failed else 0


if __name__ == "__main__":
    sys.exit(main())
