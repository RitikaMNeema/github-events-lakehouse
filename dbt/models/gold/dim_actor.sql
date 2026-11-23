{{ config(materialized='table') }}

-- Type 1 on purpose: an actor's login is their current identity and we have
-- no reliable rename signal in the event stream. Documented rather than
-- assumed, so nobody "fixes" it later by accident.
with ranked as (
    select
        actor_id,
        actor_login,
        min(event_ts) as first_seen_at,
        max(event_ts) as last_seen_at,
        count(*)      as event_count,
        row_number() over (
            partition by actor_id order by max(event_ts) desc
        ) as rn
    from {{ ref('stg_events') }}
    where actor_id is not null
    group by actor_id, actor_login
)
select
    actor_id,
    actor_login,
    first_seen_at,
    last_seen_at,
    event_count
from ranked
where rn = 1
