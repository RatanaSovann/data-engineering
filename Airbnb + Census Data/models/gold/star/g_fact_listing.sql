{{
    config(
        unique_key='id',
        alias='g_fact_listing'
    )
}}

-- Fetch necessary columns from the cleaned listings data
SELECT
    id, 
    CASE 
        WHEN listing_id IN (SELECT DISTINCT listing_id FROM {{ ref('g_dim_listing') }}) 
        THEN listing_id 
        ELSE 0 
    END AS listing_id,
    CASE 
        WHEN host_id IN (SELECT DISTINCT host_id FROM {{ ref('g_dim_host') }}) 
        THEN host_id 
        ELSE 0 
    END AS host_id,
    CASE 
        WHEN host_neighbourhood IN (SELECT DISTINCT host_neighbourhood FROM {{ ref('g_dim_host_neighbourhood') }}) 
        THEN host_neighbourhood 
        ELSE 'unknown' 
    END AS host_neighbourhood,
    CASE 
        WHEN listing_neighbourhood IN (SELECT DISTINCT listing_neighbourhood FROM {{ ref('g_dim_listing_neighbourhood') }}) 
        THEN listing_neighbourhood 
        ELSE 'unknown'
    END AS listing_neighbourhood,
    scraped_date,
    accommodates, 
    availability_30, 
    price
FROM {{ ref('s_listings') }}