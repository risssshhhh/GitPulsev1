with actors as (
    select * from {{ ref('stg_actors') }}
),
activity as (
    select * from {{ ref('int_actor_activities') }}
)
select
    a.actor_id,
    a.actor_login,
    a.actor_avatar_url,
    coalesce(act.total_events, 0) as total_events_created,
    coalesce(act.push_events_count, 0) as push_events_created,
    coalesce(act.watch_events_count, 0) as watch_events_created,
    coalesce(act.pull_request_events_count, 0) as pr_events_created,
    coalesce(act.issues_events_created, 0) as issues_events_created,
    a.last_event_at
from actors a
left join activity act on a.actor_id = act.actor_id
