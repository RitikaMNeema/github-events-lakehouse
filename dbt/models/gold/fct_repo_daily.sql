{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='repo_day_sk',
        properties={'partitioning': "ARRAY['event_date']"}
    )
}}

-- Daily rollup per repo: the table a dashboard should actually hit.
select
    {{ dbt_utils.generate_surrogate_key(['repo_id', 'event_date']) }} as repo_day_sk,
    repo_id,
    event_date,
    count(*)                                   as events,
    count(distinct actor_id)                   as distinct_actors,
    sum(if(is_star, 1, 0))                     as stars,
    sum(if(is_fork, 1, 0))                     as forks,
    sum(if(is_pr_opened, 1, 0))                as prs_opened,
    sum(if(is_pr_merged, 1, 0))                as prs_merged,
    sum(if(is_issue_opened, 1, 0))             as issues_opened,
    sum(commit_count)                          as commits,
    sum(coalesce(pr_additions, 0))             as lines_added,
    sum(coalesce(pr_deletions, 0))             as lines_deleted
from {{ ref('fct_events') }}
{% if is_incremental() %}
where event_date >= (
    select coalesce(max(event_date), date '1970-01-01')
           - interval '{{ var("lookback_days") }}' day
    from {{ this }}
)
{% endif %}
group by repo_id, event_date
