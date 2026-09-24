with actor_daily as (
    select
        actor_id,
        actor_login,
        sum(total_events) as total_events,
        sum(push_count) as push_count,
        sum(watch_count) as watch_count,
        sum(pr_count) as pr_count,
        sum(issue_count) as issue_count
    from {{ ref('fct_actor_activity') }}
    group by actor_id, actor_login
),
ranks as (
    select
        actor_id,
        actor_login,
        total_events,
        push_count,
        watch_count,
        pr_count,
        issue_count,
        rank() over (order by total_events desc) as rank_activity
    from actor_daily
)
select * from ranks
where rank_activity <= 100
