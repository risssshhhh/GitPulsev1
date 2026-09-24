with base as (
    select
        cast((raw_actor::json)->>'id' as bigint) as actor_id,
        count(*) as total_events,
        count(case when event_type = 'PushEvent' then 1 end) as push_events_count,
        count(case when event_type = 'WatchEvent' then 1 end) as watch_events_count,
        count(case when event_type = 'PullRequestEvent' then 1 end) as pull_request_events_count,
        count(case when event_type = 'IssuesEvent' then 1 end) as issues_events_count,
        max(event_at) as last_active_at
    from {{ ref('stg_raw_events') }}
    where raw_actor is not null
    group by cast((raw_actor::json)->>'id' as bigint)
)
select * from base
