with push_events as (
    select
        repo_id,
        count(*) as push_count,
        sum(coalesce(commit_size, 0)) as commit_count,
        count(distinct actor_id) as unique_pushers,
        max(event_at) as last_pushed_at
    from {{ ref('stg_push_events') }}
    group by repo_id
),
fallback as (
    select distinct
        cast((raw_repo::json)->>'id' as bigint) as repo_id
    from {{ ref('stg_raw_events') }}
)
select
    f.repo_id,
    coalesce(p.push_count, 0) as push_count,
    coalesce(p.commit_count, 0) as commit_count,
    coalesce(p.unique_pushers, 0) as unique_pushers,
    p.last_pushed_at
from fallback f
left join push_events p on f.repo_id = p.repo_id
