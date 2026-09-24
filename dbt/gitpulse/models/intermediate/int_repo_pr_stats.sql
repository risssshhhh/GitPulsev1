with pr_events as (
    select
        repo_id,
        count(case when action = 'opened' then 1 end) as prs_opened_count,
        count(case when action = 'closed' and is_merged = true then 1 end) as prs_merged_count,
        count(case when action = 'closed' and is_merged = false then 1 end) as prs_closed_count,
        avg(
            case
                when action = 'closed' and is_merged = true and pr_merged_at is not null and pr_created_at is not null
                then extract(epoch from (pr_merged_at - pr_created_at)) / 3600.0
            end
        ) as average_merge_hours
    from {{ ref('stg_pull_request_events') }}
    group by repo_id
),
fallback as (
    select distinct
        cast((raw_repo::json)->>'id' as bigint) as repo_id
    from {{ ref('stg_raw_events') }}
)
select
    f.repo_id,
    coalesce(p.prs_opened_count, 0) as prs_opened_count,
    coalesce(p.prs_merged_count, 0) as prs_merged_count,
    coalesce(p.prs_closed_count, 0) as prs_closed_count,
    coalesce(p.average_merge_hours, 0) as average_merge_hours
from fallback f
left join pr_events p on f.repo_id = p.repo_id
