{% snapshot snapshot_dim_listing_neighbourhood %}

{{
    config(
        target_schema='snapshots',
        unique_key='listing_neighbourhood',          
        strategy='timestamp',            
        updated_at='scraped_date'        
    )
}}

SELECT 
    listing_neighbourhood,                
    lga_name,                              
    lga_code,
    scraped_date        
FROM {{ ref('s_dim_listing_neighbourhood') }}      

{% endsnapshot %}