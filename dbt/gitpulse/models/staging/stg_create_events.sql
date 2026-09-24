with base as (
    select * from {{ ref('stg_raw_events') }}
    where event_type = 'CreateEvent'
)
select
    event_id,
    event_type,
    cast((raw_actor::json)->>'id' as bigint) as actor_id,
    cast((raw_repo::json)->>'id' as bigint) as repo_id,
    (raw_payload::json)->>'ref_type' as ref_type,
    (raw_payload::json)->>'ref' as ref_name,
    (raw_payload::json)->>'master_branch' as master_branch,
    (raw_payload::json)->>'description' as description,
    event_at
from base
