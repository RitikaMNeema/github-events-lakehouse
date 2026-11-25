# Dataset profile (measured)

Generated 2026-11-25 06:09 UTC by `ingest/profile_slice.py`. Source: [GH Archive](https://www.gharchive.org/).

Sampled **3 hour(s)** starting `2023-06-15T00` UTC.

## Per-hour averages

| Metric | Measured |
|---|---|
| Events per hour | 149,562 |
| Compressed (gzip) per hour | 61.6 MB |
| Uncompressed JSON per hour | 465.0 MB |
| Compression ratio | 7.5x |
| Malformed lines | 0 |

## Extrapolated volume

| Window | Compressed | Uncompressed | Events |
|---|---|---|---|
| 1 day | 1.4 GB | 10.9 GB | 3,589,480 |
| 1 month | 43.3 GB | 326.9 GB | 107,684,400 |
| 1 year | 527.3 GB | 3.9 TB | 1,310,160,200 |

> Extrapolation assumes the sampled hours are representative. They are
> not: GitHub activity is strongly diurnal and weekly, and volume has
> grown year over year. Treat these as an order of magnitude for
> capacity planning, not a forecast.

## Event-type mix

| Event type | Count | Share |
|---|---|---|
| `PushEvent` | 259,772 | 57.90% |
| `CreateEvent` | 54,187 | 12.08% |
| `PullRequestEvent` | 29,988 | 6.68% |
| `WatchEvent` | 25,905 | 5.77% |
| `DeleteEvent` | 24,903 | 5.55% |
| `IssueCommentEvent` | 17,153 | 3.82% |
| `PullRequestReviewEvent` | 8,480 | 1.89% |
| `IssuesEvent` | 6,821 | 1.52% |
| `ForkEvent` | 6,524 | 1.45% |
| `PullRequestReviewCommentEvent` | 4,273 | 0.95% |
| `CommitCommentEvent` | 4,224 | 0.94% |
| `ReleaseEvent` | 2,927 | 0.65% |
| `MemberEvent` | 1,463 | 0.33% |
| `PublicEvent` | 1,249 | 0.28% |
| `GollumEvent` | 816 | 0.18% |

## Notes for the modeller

* `WatchEvent` means **starred**, not watching. Naming it `fct_stars`
  downstream is the correct call.
* `PushEvent.payload.commits` is truncated by the source (first ~20).
  Use `payload.size` for the true commit count; the array is a sample.
* `payload` has a different shape per event type and is kept as a raw
  JSON string in bronze, typed per-type in silver.
