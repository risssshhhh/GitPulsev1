with repo_metrics as (
    select
        r.primary_language,
        sum(m.total_events) as total_events,
        sum(m.star_count) as star_count,
        sum(m.push_count) as push_count,
        sum(m.pr_count) as pr_count,
        count(distinct r.repo_id) as active_repos
    from {{ ref('fct_daily_repo_metrics') }} m
    left join {{ ref('dim_repo') }} r on m.repo_id = r.repo_id and r.is_current = true
    group by r.primary_language
)
select
    primary_language,
    total_events,
    star_count,
    push_count,
    pr_count,
    active_repos,
    rank() over (order by total_events desc) as popularity_rank
from repo_metrics
