{{ config(materialized='view') }}

-- Thin renaming/typing layer. No business logic, so that gold models never
-- reach past this into the physical table.
select
    event_id,
    event_type,
    event_ts,
    cast(event_ts as date)      as event_date,
    actor_id,
    actor_login,
    repo_id,
    repo_name,
    org_login,
    action,
    ref,
    ref_type,
    pr_number,
    pr_merged,
    pr_additions,
    pr_deletions,
    pr_changed_files,
    issue_number,
    commit_count,
    release_tag,
    source_format,
    processed_at
from {{ source('silver', 'events') }}
where repo_id is not null
