{{
    config(
        unique_key='id',
        alias='g_dim_listing'
    )
}}

WITH source  as (

    select * from {{ ref('snapshot_listing') }}

),
cleaned as (
    select
        id, 
        listing_id,
        host_id,                 
        scrape_id,
        scraped_date,                              
        listing_neighbourhood, 
        property_type,          
        room_type,                  
        has_availability,
        case when dbt_valid_from = (select min(dbt_valid_from) from source) then '1900-01-01'::timestamp else dbt_valid_from end as valid_from,
        dbt_valid_to as valid_to
    from source
),
unknown as (
    select
        '0' as id,
        0 as listing_id,
        0 as host_id,
        0 as scrape_id, 
        '1900-01-01'::timestamp  as scraped_date,
        'unknown' as listing_neighbourhood,
        'unknown' as property_type,
        'unknown' as room_type,
        TRUE as has_availability,
        '1900-01-01'::timestamp AS valid_from,
        NULL::timestamp AS valid_to
)

-- Final gold dimension: combine cleaned + unknown
SELECT * FROM cleaned
UNION ALL
SELECT * FROM unknown