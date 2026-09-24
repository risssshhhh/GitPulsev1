-- Custom singular test: valid_to must be greater than or equal to valid_from
select
    repo_id,
    valid_from,
    valid_to
from {{ ref('dim_repo') }}
where valid_to is not null and valid_to < valid_from
