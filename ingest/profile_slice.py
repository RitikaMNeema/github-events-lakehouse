"""Measure the dataset instead of guessing about it.

Streams a few hours of GH Archive, counts events, measures the compression
ratio and the event-type mix, and writes docs/_measured/dataset_profile.md.

Every size and count in the project README should come from here. "Roughly
a gigabyte an hour" is a thing you read somewhere; a measured table is a
thing you can defend.
"""
from __future__ import annotations

import argparse
import gzip
import io
import json
import logging
import os
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from common import gha_url

log = logging.getLogger("profile")
OUT = Path("/opt/project/docs/_measured/dataset_profile.md")


def profile_hour(hour: datetime) -> dict | None:
    url = gha_url(hour)
    with requests.get(url, stream=True, timeout=(10, 300)) as r:
        if r.status_code == 404:
            log.warning("missing upstream: %s", url)
            return None
        r.raise_for_status()
        r.raw.decode_content = False

        gz_bytes = 0
        raw_bytes = 0
        events = 0
        bad_lines = 0
        types: Counter[str] = Counter()
        top_keys: Counter[str] = Counter()

        # Count compressed bytes as they arrive, decompress on the fly.
        #
        # readinto, NOT read: io.BufferedReader drives a RawIOBase through
        # readinto(), and RawIOBase leaves that unimplemented, so overriding
        # only read() raises NotImplementedError from inside gzip's header
        # parse. RawIOBase.read() is itself implemented in terms of
        # readinto(), so defining readinto gives us both.
        class Counting(io.RawIOBase):
            def readinto(self, buf):
                nonlocal gz_bytes
                chunk = r.raw.read(len(buf))
                if not chunk:
                    return 0
                n = len(chunk)
                buf[:n] = chunk
                gz_bytes += n
                return n

            def readable(self):
                return True

        with gzip.open(io.BufferedReader(Counting()), "rt", encoding="utf-8") as fh:
            for line in fh:
                raw_bytes += len(line.encode("utf-8"))
                events += 1
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError:
                    bad_lines += 1
                    continue
                types[ev.get("type", "<none>")] += 1
                top_keys.update(ev.keys())

    return {
        "hour": hour,
        "url": url,
        "gz_bytes": gz_bytes,
        "raw_bytes": raw_bytes,
        "events": events,
        "bad_lines": bad_lines,
        "types": types,
        "top_keys": top_keys,
    }


def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} PB"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--hours", type=int, default=3, help="how many hours to sample")
    ap.add_argument(
        "--anchor",
        default=os.environ.get("GHA_START"),
        help="first hour to sample, YYYY-MM-DDTHH",
    )
    args = ap.parse_args()

    anchor = (
        datetime.strptime(args.anchor, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)
        if args.anchor
        else datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        - timedelta(days=2)
    )

    results = []
    for i in range(args.hours):
        res = profile_hour(anchor + timedelta(hours=i))
        if res:
            log.info(
                "%s: %s events, %s gz, %s raw",
                res["hour"],
                f"{res['events']:,}",
                human(res["gz_bytes"]),
                human(res["raw_bytes"]),
            )
            results.append(res)

    if not results:
        raise SystemExit("no hours could be profiled")

    tot_ev = sum(r["events"] for r in results)
    tot_gz = sum(r["gz_bytes"] for r in results)
    tot_raw = sum(r["raw_bytes"] for r in results)
    n = len(results)
    all_types: Counter[str] = Counter()
    for r in results:
        all_types.update(r["types"])

    avg_gz, avg_raw, avg_ev = tot_gz / n, tot_raw / n, tot_ev / n
    ratio = tot_raw / tot_gz if tot_gz else 0

    lines = [
        "# Dataset profile (measured)",
        "",
        f"Generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC by "
        "`ingest/profile_slice.py`. Source: [GH Archive](https://www.gharchive.org/).",
        "",
        f"Sampled **{n} hour(s)** starting `{anchor:%Y-%m-%dT%H}` UTC.",
        "",
        "## Per-hour averages",
        "",
        "| Metric | Measured |",
        "|---|---|",
        f"| Events per hour | {avg_ev:,.0f} |",
        f"| Compressed (gzip) per hour | {human(avg_gz)} |",
        f"| Uncompressed JSON per hour | {human(avg_raw)} |",
        f"| Compression ratio | {ratio:.1f}x |",
        f"| Malformed lines | {sum(r['bad_lines'] for r in results):,} |",
        "",
        "## Extrapolated volume",
        "",
        "| Window | Compressed | Uncompressed | Events |",
        "|---|---|---|---|",
    ]
    for label, hours in (("1 day", 24), ("1 month", 24 * 30), ("1 year", 24 * 365)):
        lines.append(
            f"| {label} | {human(avg_gz * hours)} | {human(avg_raw * hours)} "
            f"| {avg_ev * hours:,.0f} |"
        )

    lines += [
        "",
        "> Extrapolation assumes the sampled hours are representative. They are",
        "> not: GitHub activity is strongly diurnal and weekly, and volume has",
        "> grown year over year. Treat these as an order of magnitude for",
        "> capacity planning, not a forecast.",
        "",
        "## Event-type mix",
        "",
        "| Event type | Count | Share |",
        "|---|---|---|",
    ]
    for t, c in all_types.most_common():
        lines.append(f"| `{t}` | {c:,} | {100 * c / tot_ev:.2f}% |")

    lines += [
        "",
        "## Notes for the modeller",
        "",
        "* `WatchEvent` means **starred**, not watching. Naming it `fct_stars`",
        "  downstream is the correct call.",
        "* `PushEvent.payload.commits` is truncated by the source (first ~20).",
        "  Use `payload.size` for the true commit count; the array is a sample.",
        "* `payload` has a different shape per event type and is kept as a raw",
        "  JSON string in bronze, typed per-type in silver.",
        "",
    ]

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(lines), encoding="utf-8")
    log.info("wrote %s", OUT)


if __name__ == "__main__":
    main()
