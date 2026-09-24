with base as (
    select * from {{ ref('stg_raw_events') }}
    where event_type = 'PushEvent'
)
select
    event_id,
    event_type,
    cast((raw_actor::json)->>'id' as bigint) as actor_id,
    cast((raw_repo::json)->>'id' as bigint) as repo_id,
    cast((raw_payload::json)->>'push_id' as bigint) as push_id,
    cast((raw_payload::json)->>'size' as integer) as commit_size,
    cast((raw_payload::json)->>'distinct_size' as integer) as distinct_commit_size,
    (raw_payload::json)->>'ref' as branch_ref,
    event_at
from base
