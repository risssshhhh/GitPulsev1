{% snapshot dim_repo_snapshot %}

{{
    config(
      target_schema='marts',
      unique_key='repo_id',
      strategy='check',
      check_cols=['repo_name', 'primary_language', 'description', 'star_count_bucket'],
    )
}}

select
    repo_id,
    repo_name,
    primary_language,
    description,
    star_count_bucket,
    processed_at as updated_at
from {{ ref('int_daily_repo_metrics_joined') }}

{% endsnapshot %}
