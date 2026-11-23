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
select
    repo_id,
    repo_name,
    org_login,
    split_part(repo_name, '/', 1) as owner_login,
    max(event_ts)                 as last_seen_at
from {{ ref('stg_events') }}
group by repo_id, repo_name, org_login
{% endsnapshot %}
