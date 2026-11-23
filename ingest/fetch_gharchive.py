"""Fetch GH Archive hourly files into the raw zone.

Properties that matter for the interview conversation:

  * Idempotent. Re-running a range re-uploads nothing: each hour is
    HEAD-checked against the source Content-Length first.
  * Honest about gaps. GH Archive is genuinely missing some hours from
    upstream outages. A 404 is recorded as `missing_upstream`, NOT as a
    failure, so the quality gate can tell a real incident apart from a
    known hole in the source.
  * Streamed. The body goes source -> object storage without ever landing
    on local disk, so a wide backfill does not need wide disk.
  * Raw is immutable. We never parse or reshape here. Reprocessing a
    schema decision must never require re-downloading the internet.
"""
from __future__ import annotations

import argparse
import logging
import sys

import requests
from botocore.exceptions import ClientError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from common import (
    Settings,
    add_range_args,
    ensure_manifest,
    gha_url,
    hour_range,
    raw_key,
    record,
    resolve_range,
    s3_client,
)

log = logging.getLogger("fetch")

TIMEOUT = (10, 300)  # connect, read


class Transient(Exception):
    pass


@retry(
    retry=retry_if_exception_type(Transient),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)
def source_size(url: str) -> int | None:
    """Content-Length of the upstream object, or None if it isn't there."""
    r = requests.head(url, timeout=TIMEOUT, allow_redirects=True)
    if r.status_code == 404:
        return None
    if r.status_code >= 500 or r.status_code == 429:
        raise Transient(f"{r.status_code} on HEAD {url}")
    r.raise_for_status()
    cl = r.headers.get("Content-Length")
    return int(cl) if cl else -1  # -1: unknown length, cannot size-compare


def existing_size(s3, bucket: str, key: str) -> int | None:
    try:
        return s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


@retry(
    retry=retry_if_exception_type(Transient),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)
def stream_to_s3(s3, url: str, bucket: str, key: str) -> tuple[int, str]:
    with requests.get(url, stream=True, timeout=TIMEOUT) as r:
        if r.status_code >= 500 or r.status_code == 429:
            raise Transient(f"{r.status_code} on GET {url}")
        r.raise_for_status()
        r.raw.decode_content = False  # keep it gzipped; raw stays as-sent
        s3.upload_fileobj(r.raw, bucket, key)
    head = s3.head_object(Bucket=bucket, Key=key)
    return head["ContentLength"], head["ETag"].strip('"')


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    p = add_range_args(argparse.ArgumentParser(description=__doc__))
    p.add_argument(
        "--force", action="store_true", help="re-download even if already present"
    )
    args = p.parse_args()

    cfg = Settings.from_env()
    start, end = resolve_range(args)
    ensure_manifest(cfg)
    s3 = s3_client(cfg)

    tally = {"fetched": 0, "present": 0, "missing_upstream": 0, "failed": 0}

    for hour in hour_range(start, end):
        url, key = gha_url(hour), raw_key(hour)
        try:
            upstream = source_size(url)
            if upstream is None:
                log.warning("hour missing upstream: %s", url)
                record(cfg, hour, url, None, None, None, "missing_upstream")
                tally["missing_upstream"] += 1
                continue

            have = existing_size(s3, cfg.bucket, key)
            if have is not None and not args.force and (upstream in (-1, have)):
                log.info("present, skipping: %s (%s bytes)", key, have)
                record(cfg, hour, url, key, have, None, "present")
                tally["present"] += 1
                continue

            size, etag = stream_to_s3(s3, url, cfg.bucket, key)
            log.info("fetched %s -> %s (%s bytes)", url, key, size)
            record(cfg, hour, url, key, size, etag, "fetched")
            tally["fetched"] += 1

        except Exception as exc:  # noqa: BLE001 - one bad hour must not kill a backfill
            log.error("FAILED %s: %s", url, exc)
            record(cfg, hour, url, key, None, None, "failed")
            tally["failed"] += 1

    log.info("summary %s", tally)
    # Non-zero only on real failures. Upstream gaps are data, not errors.
    return 1 if tally["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
