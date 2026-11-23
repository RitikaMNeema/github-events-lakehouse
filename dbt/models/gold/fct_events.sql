{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='event_id',
        properties={
            'format': "'PARQUET'",
            'partitioning': "ARRAY['day(event_ts)']",
            'sorted_by': "ARRAY['repo_id']"
        }
    )
}}

-- The grain is one GitHub event. Partitioned by event day and sorted by
-- repo_id, because every query in docs/data-contract.md filters on a date
-- range and then groups by repo.
select
    e.event_id,
    e.event_ts,
    e.event_date,
    e.event_type,
    e.actor_id,
    e.repo_id,
    e.org_login,
    e.action,
    -- WatchEvent is a STAR, not a watch. Naming it correctly here stops the
    -- misunderstanding propagating into every downstream metric.
    e.event_type = 'WatchEvent'                            as is_star,
    e.event_type = 'ForkEvent'                             as is_fork,
    e.event_type = 'PullRequestEvent' and e.action = 'opened' as is_pr_opened,
    e.event_type = 'PullRequestEvent' and e.pr_merged      as is_pr_merged,
    e.event_type = 'IssuesEvent' and e.action = 'opened'    as is_issue_opened,
    -- True push size from payload.size. The commits array is truncated by
    -- the source, so counting silver.commits rows would undercount.
    case when e.event_type = 'PushEvent' then e.commit_count else 0 end as commit_count,
    e.pr_additions,
    e.pr_deletions,
    e.pr_changed_files,
    e.source_format,
    e.processed_at
from {{ ref('stg_events') }} e

{% if is_incremental() %}
  -- Bounded lookback rather than a full rescan: cheap, and still catches
  -- events that arrived late into an already-processed day.
  where e.event_ts >= (
      select coalesce(max(event_ts), timestamp '1970-01-01 00:00:00')
             - interval '{{ var("lookback_days") }}' day
      from {{ this }}
  )
{% endif %}
