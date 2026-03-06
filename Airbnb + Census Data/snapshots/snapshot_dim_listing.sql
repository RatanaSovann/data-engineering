{% snapshot snapshot_listing %}

{{
    config(
        target_schema='snapshots',     
        unique_key='id',        
        strategy='timestamp',          
        updated_at='scraped_date'    
    )
}}

SELECT
    id, 
    listing_id,               
    host_id,                   
    scrape_id,                  
    scraped_date,
    listing_neighbourhood, 
    property_type,          
    room_type,                 
    has_availability
FROM {{ ref('s_dim_listing') }}

{% endsnapshot %}