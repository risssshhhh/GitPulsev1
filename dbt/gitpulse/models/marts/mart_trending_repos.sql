with repo_daily as (
    select
        repo_id,
        repo_name,
        sum(total_events) as total_events,
        sum(star_count) as star_growth,
        sum(push_count) as push_growth,
        sum(pr_count) as pr_growth
    from {{ ref('fct_daily_repo_metrics') }}
    group by repo_id, repo_name
),
ranks as (
    select
        repo_id,
        repo_name,
        total_events,
        star_growth,
        push_growth,
        pr_growth,
        rank() over (order by total_events desc) as rank_total,
        rank() over (order by star_growth desc) as rank_stars
    from repo_daily
)
select * from ranks
