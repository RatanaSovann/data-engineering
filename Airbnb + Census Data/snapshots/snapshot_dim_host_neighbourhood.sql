{% snapshot snapshot_dim_host_neighbourhood %}

{{
    config(
        target_schema='snapshots',
        unique_key='host_neighbourhood',          
        strategy='timestamp',            
        updated_at='scraped_date'        
    )
}}

SELECT
    host_neighbourhood,                 
    lga_name,                             
    lga_code,
    scraped_date        
FROM {{ ref('s_dim_host_neighbourhood') }}      

{% endsnapshot %}