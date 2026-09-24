with snapshot as (
    select * from {{ ref('dim_repo_snapshot') }}
)
select
    repo_id,
    repo_name,
    primary_language,
    description,
    star_count_bucket,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    case when dbt_valid_to is null then true else false end as is_current
from snapshot
