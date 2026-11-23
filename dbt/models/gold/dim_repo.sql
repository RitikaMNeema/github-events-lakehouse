{{ config(materialized='table') }}

-- Presentation layer over the snapshot: stable surrogate key, readable
-- validity window, and a current-row flag so the common case is one filter.
select
    {{ dbt_utils.generate_surrogate_key(['repo_id', 'dbt_valid_from']) }} as repo_sk,
    repo_id,
    repo_name,
    owner_login,
    org_login,
    last_seen_at,
    dbt_valid_from                              as valid_from,
    coalesce(dbt_valid_to, timestamp '9999-12-31 00:00:00') as valid_to,
    dbt_valid_to is null                        as is_current
from {{ ref('scd_repo') }}
