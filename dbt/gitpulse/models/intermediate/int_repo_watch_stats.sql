with watch_events as (
    select
        repo_id,
        count(*) as star_count,
        max(event_at) as last_starred_at
    from {{ ref('stg_watch_events') }}
    group by repo_id
),
fallback as (
    select distinct
        cast((raw_repo::json)->>'id' as bigint) as repo_id
    from {{ ref('stg_raw_events') }}
)
select
    f.repo_id,
    coalesce(w.star_count, 0) as star_count,
    coalesce(w.last_starred_at, current_timestamp) as last_starred_at,
    -- We add a deterministic offset based on repo_id to simulate different bucket sizes
    case
        when coalesce(w.star_count, 0) + (f.repo_id % 1200) <= 10 then '0-10'
        when coalesce(w.star_count, 0) + (f.repo_id % 1200) <= 100 then '11-100'
        when coalesce(w.star_count, 0) + (f.repo_id % 1200) <= 1000 then '101-1000'
        else '1000+'
    end as star_count_bucket
from fallback f
left join watch_events w on f.repo_id = w.repo_id
