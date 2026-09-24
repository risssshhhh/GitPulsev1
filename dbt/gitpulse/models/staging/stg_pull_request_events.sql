with base as (
    select * from {{ ref('stg_raw_events') }}
    where event_type = 'PullRequestEvent'
)
select
    event_id,
    event_type,
    cast((raw_actor::json)->>'id' as bigint) as actor_id,
    cast((raw_repo::json)->>'id' as bigint) as repo_id,
    (raw_payload::json)->>'action' as action,
    cast(((raw_payload::json)->'pull_request'->>'id') as bigint) as pr_id,
    cast(((raw_payload::json)->'pull_request'->>'number') as integer) as pr_number,
    (raw_payload::json)->'pull_request'->>'state' as pr_state,
    cast(((raw_payload::json)->'pull_request'->>'merged') as boolean) as is_merged,
    case
        when (raw_payload::json)->'pull_request'->>'created_at' like '%T%' then to_timestamp((raw_payload::json)->'pull_request'->>'created_at', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
        else to_timestamp((raw_payload::json)->'pull_request'->>'created_at', 'YYYY-MM-DD HH24:MI:SS')
    end as pr_created_at,
    case
        when (raw_payload::json)->'pull_request'->>'closed_at' is null then null
        when (raw_payload::json)->'pull_request'->>'closed_at' like '%T%' then to_timestamp((raw_payload::json)->'pull_request'->>'closed_at', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
        else to_timestamp((raw_payload::json)->'pull_request'->>'closed_at', 'YYYY-MM-DD HH24:MI:SS')
    end as pr_closed_at,
    case
        when (raw_payload::json)->'pull_request'->>'merged_at' is null then null
        when (raw_payload::json)->'pull_request'->>'merged_at' like '%T%' then to_timestamp((raw_payload::json)->'pull_request'->>'merged_at', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
        else to_timestamp((raw_payload::json)->'pull_request'->>'merged_at', 'YYYY-MM-DD HH24:MI:SS')
    end as pr_merged_at,
    event_at
from base
