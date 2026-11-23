"""Expire the raw zone once bronze provably holds the same information.

The premise: you do not have to STORE data to have PROCESSED it. Streaming
a year of source JSON through the pipeline and retaining only the columnar
layers is how real platforms work -- tiered retention with raw expiry.

The danger: bronze persists only the top-level keys someone thought to map.
If the source carries a field the schema never captured, deleting raw
destroys data nobody knows existed, and no amount of reprocessing brings it
back. "Raw is my insurance policy" and "delete raw" can only both be true
if bronze is PROVABLY lossless.

So every hour must clear four gates before its object is deleted:

  1. AGE       older than --min-age-hours. Recent data stays; a bug found
               tomorrow should still be fixable from source.
  2. BRONZE    rows exist in bronze whose source_file is this object.
  3. SILVER    the hour made it through normalisation and the quality gate.
  4. LOSSLESS  no row in the hour reports an unmapped top-level key, unless
               that key was explicitly approved via --discard-keys.

Gate 4 is the one that matters. Discarding a field is a decision, and this
job makes you state it on the command line rather than discover it later.

Dry-run by default. Pass --apply to actually delete.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, "/opt/project/ingest")
sys.path.insert(0, "/opt/project/spark/jobs")

from common import (  # noqa: E402
    Settings,
    add_range_args,
    manifest_rows,
    mark_status,
    raw_key,
    resolve_range,
    s3_client,
)
from session import build  # noqa: E402

log = logging.getLogger("expire-raw")
OUT = Path("/opt/project/docs/_measured/retention.md")

EXPIRED = "raw_expired"


def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} PB"


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ap = add_range_args(argparse.ArgumentParser(description=__doc__))
    ap.add_argument(
        "--min-age-hours",
        type=int,
        default=48,
        help="never expire raw younger than this (default 48)",
    )
    ap.add_argument(
        "--apply",
        action="store_true",
        help="actually delete. Without it, nothing is removed.",
    )
    ap.add_argument(
        "--discard-keys",
        default="",
        help="comma-separated top-level source keys you ACCEPT losing, "
        "e.g. 'url,actor_attributes'. Each one is recorded in the report.",
    )
    ap.add_argument(
        "--no-require-silver",
        action="store_true",
        help="skip the silver-coverage gate (not recommended)",
    )
    args = ap.parse_args()

    cfg = Settings.from_env()
    start, end = resolve_range(args)
    approved = {k.strip() for k in args.discard_keys.split(",") if k.strip()}
    cutoff = datetime.now(timezone.utc) - timedelta(hours=args.min_age_hours)

    spark = build("expire_raw", cfg)
    cat = cfg.catalog
    s3 = s3_client(cfg)

    rows = manifest_rows(cfg, start, end)
    if not rows:
        log.error("no manifest rows in range - has ingest run?")
        spark.stop()
        return 1

    expired, refused, reclaimed = [], [], 0

    for m in rows:
        hour, key, size, status = (
            m["hour_ts"],
            m["object_key"],
            m["size_bytes"] or 0,
            m["status"],
        )
        tag = f"{hour:%Y-%m-%dT%H}"

        if status == EXPIRED:
            continue
        if status == "missing_upstream":
            continue  # nothing was ever stored
        if not key:
            refused.append((tag, 0, "no object key recorded"))
            continue

        # --- gate 1: age
        if hour > cutoff:
            refused.append((tag, size, f"younger than {args.min_age_hours}h"))
            continue

        # --- gate 2: bronze coverage
        expected_key = raw_key(hour)
        bronze = spark.sql(
            f"""SELECT count(*) c, max(size(unmapped_keys)) mx
                FROM {cat}.bronze.gh_events
                WHERE ingest_date = date'{hour:%Y-%m-%d}'
                  AND ingest_hour = {hour.hour}
                  AND source_file LIKE '%{expected_key}'"""
        ).collect()[0]
        if not bronze["c"]:
            refused.append((tag, size, "no bronze rows for this object"))
            continue

        # --- gate 3: silver coverage
        if not args.no_require_silver:
            sc = spark.sql(
                f"""SELECT count(*) c FROM {cat}.silver.events
                    WHERE source_file LIKE '%{expected_key}'"""
            ).collect()[0]["c"]
            if not sc:
                refused.append((tag, size, "no silver rows - not yet normalised"))
                continue

        # --- gate 4: losslessness
        if bronze["mx"]:
            found = spark.sql(
                f"""SELECT DISTINCT explode(unmapped_keys) k
                    FROM {cat}.bronze.gh_events
                    WHERE ingest_date = date'{hour:%Y-%m-%d}'
                      AND ingest_hour = {hour.hour}
                      AND source_file LIKE '%{expected_key}'"""
            ).collect()
            keys = {r["k"] for r in found if r["k"]}
            unapproved = keys - approved
            if unapproved:
                refused.append(
                    (tag, size, f"unmapped keys not approved: {sorted(unapproved)}")
                )
                continue

        # --- all gates cleared
        if args.apply:
            s3.delete_object(Bucket=cfg.bucket, Key=key)
            mark_status(cfg, hour, EXPIRED)
            log.info("expired %s (%s reclaimed)", tag, human(size))
        else:
            log.info("WOULD expire %s (%s)", tag, human(size))
        expired.append((tag, size))
        reclaimed += size

    # ---------------------------------------------------------------- report
    still_held = sum(
        (m["size_bytes"] or 0) for m in rows if m["status"] not in (EXPIRED, "missing_upstream")
    ) - (reclaimed if args.apply else 0)
    processed_total = sum((m["size_bytes"] or 0) for m in rows)

    mode = "APPLIED" if args.apply else "DRY RUN (nothing deleted)"
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(
        "\n".join(
            [
                "# Raw-zone retention",
                "",
                f"**{mode}** -- {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC, "
                f"range `{start:%Y-%m-%dT%H}` .. `{end:%Y-%m-%dT%H}`.",
                "",
                "## Policy",
                "",
                f"* Minimum age before expiry: **{args.min_age_hours}h**",
                f"* Silver coverage required: **{not args.no_require_silver}**",
                "* Keys explicitly approved for discard: "
                + (f"`{'`, `'.join(sorted(approved))}`" if approved else "**none**"),
                "",
                "## Result",
                "",
                "| Measure | Value |",
                "|---|---|",
                f"| Source bytes processed (all time, this range) | {human(processed_total)} |",
                f"| Hours expired | {len(expired)} |",
                f"| Bytes reclaimed | {human(reclaimed)} |",
                f"| Raw still retained | {human(max(still_held, 0))} |",
                f"| Hours refused | {len(refused)} |",
                "",
                "## Refusals",
                "",
                "| Hour | Size | Why raw was kept |",
                "|---|---|---|",
                *(
                    [f"| `{t}` | {human(sz)} | {why} |" for t, sz, why in refused[:40]]
                    or ["| _none_ | | |"]
                ),
                "",
                "A refusal is the system working. The commonest one is an "
                "unmapped top-level key, which means bronze is **not** a "
                "lossless re-encoding of those records and the raw object is "
                "still the only copy of something. Either map the field in "
                "`PERSISTED_KEYS`, or decide out loud to lose it with "
                "`--discard-keys`.",
                "",
                "## Why bytes_processed survives deletion",
                "",
                "`ingest_manifest.size_bytes` is never cleared on expiry. Once "
                "the object is gone, that column is the only remaining record "
                "of how much source data moved through the pipeline -- so it "
                "is the ledger behind any throughput claim. See "
                "`tests/report_throughput.py`.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    log.info(
        "%s: %d expired (%s), %d refused, wrote %s",
        mode, len(expired), human(reclaimed), len(refused), OUT,
    )
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
