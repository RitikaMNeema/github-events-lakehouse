{{ config(materialized='view') }}

select
    event_id,
    event_ts,
    repo_id,
    repo_name,
    actor_login,
    commit_sha,
    author_name,
    lower(author_email)         as author_email,
    message,
    is_distinct
from {{ source('silver', 'commits') }}
