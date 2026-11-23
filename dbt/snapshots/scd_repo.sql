{% snapshot scd_repo %}
{{
    config(
        unique_key='repo_id',
        strategy='check',
        check_cols=['repo_name', 'org_login'],
        invalidate_hard_deletes=False
    )
}}

-- SCD Type 2 on the repository.
--
-- Repos get renamed and transferred between owners, and the event log
-- records the name as it was AT THAT MOMENT. A Type 1 dimension would
-- rewrite history: last month's commits would appear under this month's
-- name, and "which org owned this repo when the incident happened" becomes
-- unanswerable. Type 2 keeps both.
--
-- Hard deletes are NOT invalidated: a repo going private disappears from
-- the public event stream, which is not the same as ceasing to exist.
--
-- ONE ROW PER repo_id IS MANDATORY.
--
-- The obvious version of this query -- GROUP BY repo_id, repo_name,
-- org_login -- is wrong, and fails loudly:
--
--     MERGE_TARGET_ROW_MULTIPLE_MATCHES:
--     One MERGE target table row matched more than one source row
--
-- because a repo observed under two names (a rename inside the window) or
-- with org_login populated on some events and null on others yields
-- several source rows for a single target row, and the snapshot merges on
-- repo_id alone. Collapsing to the most recent observation per repo_id is
-- also the semantically correct choice: the snapshot captures current
-- state, and dbt derives the history from the differences between runs.
with observations as (
    select
        repo_id,
        repo_name,
        org_login,
        event_ts,
        row_number() over (
            partition by repo_id
            -- event_ts alone can tie when a repo has several events in the
            -- same instant; repo_name breaks it deterministically so two
            -- runs over identical data cannot disagree about which name won.
            order by event_ts desc, repo_name desc
        ) as rn
    from {{ ref('stg_events') }}
    where repo_id is not null
)

select
    repo_id,
    repo_name,
    org_login,
    split_part(repo_name, '/', 1) as owner_login,
    event_ts                      as last_seen_at
from observations
where rn = 1
{% endsnapshot %}
