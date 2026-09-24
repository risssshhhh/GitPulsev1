with issue_events as (
    select
        repo_id,
        count(case when action = 'opened' then 1 end) as opened_issues_count,
        count(case when action = 'closed' then 1 end) as closed_issues_count,
        avg(
            case
                when action = 'closed' and issue_closed_at is not null and issue_created_at is not null
                then extract(epoch from (issue_closed_at - issue_created_at)) / 3600.0
            end
        ) as average_resolution_hours
    from {{ ref('stg_issues_events') }}
    group by repo_id
),
fallback as (
    select distinct
        cast((raw_repo::json)->>'id' as bigint) as repo_id
    from {{ ref('stg_raw_events') }}
)
select
    f.repo_id,
    coalesce(i.opened_issues_count, 0) as opened_issues_count,
    coalesce(i.closed_issues_count, 0) as closed_issues_count,
    coalesce(i.average_resolution_hours, 0) as average_resolution_hours
from fallback f
left join issue_events i on f.repo_id = i.repo_id
