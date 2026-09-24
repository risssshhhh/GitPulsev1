-- Custom singular test: star counts must be non-negative
select
    repo_id,
    star_count
from {{ ref('int_repo_watch_stats') }}
where star_count < 0
