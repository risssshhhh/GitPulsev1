with source as (
    select * from {{ source('raw', 'github_events') }}
),
staged as (
    select
        id as event_id,
        type as event_type,
        actor as raw_actor,
        repo as raw_repo,
        org as raw_org,
        payload as raw_payload,
        public as is_public,
        case
            when created_at like '%T%' then to_timestamp(created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
            else to_timestamp(created_at, 'YYYY-MM-DD HH24:MI:SS')
        end as event_at,
        loaded_at
    from source
)
select * from staged
