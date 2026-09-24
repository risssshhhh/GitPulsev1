with source as (
    select * from {{ ref('stg_raw_events') }}
),
parsed as (
    select
        cast((raw_actor::json)->>'id' as bigint) as actor_id,
        (raw_actor::json)->>'login' as actor_login,
        (raw_actor::json)->>'avatar_url' as actor_avatar_url,
        event_at,
        row_number() over (
            partition by cast((raw_actor::json)->>'id' as bigint)
            order by event_at desc
        ) as rn
    from source
    where raw_actor is not null
)
select
    actor_id,
    actor_login,
    actor_avatar_url,
    event_at as last_event_at
from parsed
where rn = 1
