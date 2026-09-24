with base as (
    select * from {{ ref('stg_raw_events') }}
    where event_type = 'ForkEvent'
)
select
    event_id,
    event_type,
    cast((raw_actor::json)->>'id' as bigint) as actor_id,
    cast((raw_repo::json)->>'id' as bigint) as repo_id,
    cast(((raw_payload::json)->'forkee'->>'id') as bigint) as forkee_id,
    (raw_payload::json)->'forkee'->>'full_name' as forkee_name,
    event_at
from base
