select
    event_id,
    repo_id,
    actor_id,
    event_at as starred_at
from {{ ref('stg_watch_events') }}
