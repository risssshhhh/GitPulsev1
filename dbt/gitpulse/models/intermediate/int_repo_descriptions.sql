with create_desc as (
    select
        repo_id,
        description,
        event_at,
        row_number() over (partition by repo_id order by event_at desc) as rn
    from {{ ref('stg_create_events') }}
    where description is not null
),
fallback as (
    select
        cast((raw_repo::json)->>'id' as bigint) as repo_id,
        'Description default for repository ' || (raw_repo::json)->>'name' as description,
        event_at,
        row_number() over (
            partition by cast((raw_repo::json)->>'id' as bigint)
            order by event_at desc
        ) as rn
    from {{ ref('stg_raw_events') }}
)
select
    f.repo_id,
    coalesce(c.description, f.description) as description,
    coalesce(c.event_at, f.event_at) as updated_at
from fallback f
left join create_desc c on f.repo_id = c.repo_id and c.rn = 1
where f.rn = 1
