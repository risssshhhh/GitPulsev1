with repos as (
    select distinct
        repo_id,
        repo_name
    from {{ ref('stg_repos') }}
),
langs as (
    select * from {{ ref('int_repo_languages') }}
),
descs as (
    select * from {{ ref('int_repo_descriptions') }}
),
watches as (
    select * from {{ ref('int_repo_watch_stats') }}
),
pushes as (
    select * from {{ ref('int_repo_push_stats') }}
),
issues as (
    select * from {{ ref('int_repo_issue_stats') }}
),
prs as (
    select * from {{ ref('int_repo_pr_stats') }}
)
select
    r.repo_id,
    r.repo_name,
    coalesce(l.primary_language, 'Unknown') as primary_language,
    coalesce(d.description, '') as description,
    coalesce(w.star_count, 0) as star_count,
    coalesce(w.star_count_bucket, '0-10') as star_count_bucket,
    coalesce(p.push_count, 0) as push_count,
    coalesce(p.commit_count, 0) as commit_count,
    coalesce(p.unique_pushers, 0) as unique_pushers,
    coalesce(i.opened_issues_count, 0) as opened_issues_count,
    coalesce(i.closed_issues_count, 0) as closed_issues_count,
    coalesce(i.average_resolution_hours, 0.0) as average_resolution_hours,
    coalesce(pr.prs_opened_count, 0) as prs_opened_count,
    coalesce(pr.prs_merged_count, 0) as prs_merged_count,
    coalesce(pr.prs_closed_count, 0) as prs_closed_count,
    coalesce(pr.average_merge_hours, 0.0) as average_merge_hours,
    current_timestamp as processed_at
from repos r
left join langs l on r.repo_id = l.repo_id
left join descs d on r.repo_id = d.repo_id
left join watches w on r.repo_id = w.repo_id
left join pushes p on r.repo_id = p.repo_id
left join issues i on r.repo_id = i.repo_id
left join prs pr on r.repo_id = pr.repo_id
