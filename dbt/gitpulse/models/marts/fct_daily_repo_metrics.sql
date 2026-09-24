with daily_events as (
    select
        repo_id,
        event_at::date as event_date,
        count(*) as total_events,
        count(case when event_type = 'WatchEvent' then 1 end) as star_count,
        count(case when event_type = 'PushEvent' then 1 end) as push_count,
        count(case when event_type = 'PullRequestEvent' then 1 end) as pr_count,
        count(case when event_type = 'IssuesEvent' then 1 end) as issue_count
    from {{ ref('stg_raw_events') }}
    group by repo_id, event_at::date
),
repo_info as (
    select distinct
        repo_id,
        repo_name
    from {{ ref('stg_repos') }}
)
select
    d.repo_id,
    r.repo_name,
    d.event_date,
    d.total_events,
    d.star_count,
    d.push_count,
    d.pr_count,
    d.issue_count
from daily_events d
left join repo_info r on d.repo_id = r.repo_id
