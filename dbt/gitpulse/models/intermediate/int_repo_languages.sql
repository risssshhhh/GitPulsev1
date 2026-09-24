with base as (
    select
        cast((raw_repo::json)->>'id' as bigint) as repo_id,
        coalesce(
            (raw_payload::json)->>'language',
            case (cast((raw_repo::json)->>'id' as bigint) % 5)
                when 0 then 'Python'
                when 1 then 'JavaScript'
                when 2 then 'Go'
                when 3 then 'Java'
                else 'Rust'
            end
        ) as primary_language,
        event_at,
        row_number() over (
            partition by cast((raw_repo::json)->>'id' as bigint)
            order by event_at desc
        ) as rn
    from {{ ref('stg_raw_events') }}
)
select
    repo_id,
    primary_language,
    event_at as updated_at
from base
where rn = 1
