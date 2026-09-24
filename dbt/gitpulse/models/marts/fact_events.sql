with base_events as (
    select
        event_id,
        event_type,
        cast((raw_actor::json)->>'id' as bigint) as actor_id,
        cast((raw_repo::json)->>'id' as bigint) as repo_id,
        event_at,
        is_public,
        loaded_at
    from {{ ref('stg_raw_events') }}
)
select
    event_id,
    event_type,
    actor_id,
    repo_id,
    event_at::date as date_id,
    event_at,
    is_public,
    loaded_at
from base_events
