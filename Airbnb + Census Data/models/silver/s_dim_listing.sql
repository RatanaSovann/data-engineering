{{ 
    config(
        alias='s_dim_listing',
        unique_key='id'
    ) 
}}

WITH source AS (
    SELECT *
    FROM {{ ref('b_listings') }}
),

cleaned_data AS (
    SELECT
        -- Keep surrogate key from bronze
        id,

        -- Cast numeric fields
        CAST(listing_id AS INT) AS listing_id,
        CAST(host_id AS INT) AS host_id,
        CAST(scrape_id AS BIGINT) AS scrape_id,

        -- Handle multiple date formats and NaNs
        CASE 
            WHEN scraped_date IS NULL OR scraped_date IN ('NaN', '') THEN NOW()::timestamp
            WHEN POSITION('/' IN scraped_date) > 0 THEN TO_TIMESTAMP(scraped_date, 'DD/MM/YYYY')
            ELSE TO_TIMESTAMP(scraped_date, 'YYYY-MM-DD')
        END AS scraped_date,

        -- Standardize text columns
        LOWER(TRIM(property_type)) AS property_type,
        LOWER(TRIM(room_type)) AS room_type,

        -- Convert availability flag to boolean
        CASE 
            WHEN LOWER(TRIM(has_availability)) IN ('t', 'true', 'yes', '1') THEN TRUE
            ELSE FALSE
        END AS has_availability,

        -- Clean neighbourhoods
        CASE 
            WHEN listing_neighbourhood IS NULL 
                 OR LOWER(TRIM(listing_neighbourhood)) IN ('nan', 'unknown', '') 
                 THEN 'no neighbourhood'
            ELSE LOWER(TRIM(listing_neighbourhood))
        END AS listing_neighbourhood
    FROM source
),

joined AS (
    -- Join to neighbourhood → LGA mapping
    SELECT 
        c.id,
        c.listing_id,
        c.host_id,
        c.scrape_id,
        c.scraped_date,
        c.property_type,
        c.room_type,
        c.has_availability,
        c.listing_neighbourhood
    FROM cleaned_data c
    LEFT JOIN {{ ref('s_dim_listing_neighbourhood') }} ln
        ON c.listing_neighbourhood = ln.listing_neighbourhood
)

-- Filter invalid records
SELECT *
FROM joined
WHERE listing_id IS NOT NULL

