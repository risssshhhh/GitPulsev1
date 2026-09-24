with daily_actor_events as (
    select
        cast((raw_actor::json)->>'id' as bigint) as actor_id,
        event_at::date as event_date,
        count(*) as total_events,
        count(case when event_type = 'PushEvent' then 1 end) as push_count,
        count(case when event_type = 'WatchEvent' then 1 end) as watch_count,
        count(case when event_type = 'PullRequestEvent' then 1 end) as pr_count,
        count(case when event_type = 'IssuesEvent' then 1 end) as issue_count
    from {{ ref('stg_raw_events') }}
    where raw_actor is not null
    group by cast((raw_actor::json)->>'id' as bigint), event_at::date
),
actor_info as (
    select distinct
        actor_id,
        actor_login
    from {{ ref('stg_actors') }}
)
select
    d.actor_id,
    a.actor_login,
    d.event_date,
    d.total_events,
    d.push_count,
    d.watch_count,
    d.pr_count,
    d.issue_count
from daily_actor_events d
left join actor_info a on d.actor_id = a.actor_id
