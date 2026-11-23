-- Singular test: no gaps or overlaps in the SCD2 validity timeline.
with windows as (
    select
        repo_id,
        valid_from,
        valid_to,
        lead(valid_from) over (partition by repo_id order by valid_from) as next_from
    from {{ ref('dim_repo') }}
)
select *
from windows
where next_from is not null
  and valid_to <> next_from
