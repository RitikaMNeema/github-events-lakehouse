-- Singular test: SCD2 integrity.
-- An SCD2 dimension with two "current" rows for one key is the classic
-- silent corruption - every join fans out and every metric doubles.
select repo_id, count(*) as current_rows
from {{ ref('dim_repo') }}
where is_current
group by repo_id
having count(*) > 1
