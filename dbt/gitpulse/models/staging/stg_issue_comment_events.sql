with base as (
    select * from {{ ref('stg_raw_events') }}
    where event_type = 'IssueCommentEvent'
)
select
    event_id,
    event_type,
    cast((raw_actor::json)->>'id' as bigint) as actor_id,
    cast((raw_repo::json)->>'id' as bigint) as repo_id,
    (raw_payload::json)->>'action' as action,
    cast(((raw_payload::json)->'issue'->>'id') as bigint) as issue_id,
    cast(((raw_payload::json)->'comment'->>'id') as bigint) as comment_id,
    event_at
from base
