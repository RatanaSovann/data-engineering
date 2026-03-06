{% snapshot snapshot_host %}

{{
    config(
        target_schema='snapshots',
        unique_key='host_id',
        strategy='timestamp',
        updated_at='scraped_date',
        invalidate_hard_deletes=True
    )
}}

SELECT 
    host_id,
    host_name,
    host_since,
    is_superhost,
    host_neighbourhood,
    scraped_date
FROM {{ ref('s_dim_host') }}

{% endsnapshot %}