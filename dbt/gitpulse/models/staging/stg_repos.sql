with source as (
    select * from {{ ref('stg_raw_events') }}
),
parsed as (
    select
        cast((raw_repo::json)->>'id' as bigint) as repo_id,
        (raw_repo::json)->>'name' as repo_name,
        event_at,
        row_number() over (
            partition by cast((raw_repo::json)->>'id' as bigint)
            order by event_at desc
        ) as rn
    from source
    where raw_repo is not null
)
select
    repo_id,
    repo_name,
    event_at as last_event_at
from parsed
where rn = 1
