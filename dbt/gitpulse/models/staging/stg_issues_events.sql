with base as (
    select * from {{ ref('stg_raw_events') }}
    where event_type = 'IssuesEvent'
)
select
    event_id,
    event_type,
    cast((raw_actor::json)->>'id' as bigint) as actor_id,
    cast((raw_repo::json)->>'id' as bigint) as repo_id,
    (raw_payload::json)->>'action' as action,
    cast(((raw_payload::json)->'issue'->>'id') as bigint) as issue_id,
    cast(((raw_payload::json)->'issue'->>'number') as integer) as issue_number,
    (raw_payload::json)->'issue'->>'state' as issue_state,
    case
        when (raw_payload::json)->'issue'->>'created_at' like '%T%' then to_timestamp((raw_payload::json)->'issue'->>'created_at', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
        else to_timestamp((raw_payload::json)->'issue'->>'created_at', 'YYYY-MM-DD HH24:MI:SS')
    end as issue_created_at,
    case
        when (raw_payload::json)->'issue'->>'closed_at' is null then null
        when (raw_payload::json)->'issue'->>'closed_at' like '%T%' then to_timestamp((raw_payload::json)->'issue'->>'closed_at', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
        else to_timestamp((raw_payload::json)->'issue'->>'closed_at', 'YYYY-MM-DD HH24:MI:SS')
    end as issue_closed_at,
    event_at
from base
