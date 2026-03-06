{{
    config(
        unique_key='listing_neighbourhood',
        alias='s_dim_listing_neighbourhood',
    )
}}

WITH listings AS (
    SELECT DISTINCT
        -- Standardize and clean neighbourhood names
        CASE 
            WHEN listing_neighbourhood IS NULL 
                 OR TRIM(LOWER(listing_neighbourhood)) IN ('nan', 'none', 'overseas', '') 
            THEN 'no address'
            ELSE TRIM(LOWER(listing_neighbourhood))
        END AS listing_neighbourhood,

        -- Handle different date formats and invalid values
        MAX(
            CASE 
                WHEN scraped_date IS NULL OR TRIM(LOWER(scraped_date)) = 'nan' THEN NOW()::timestamp
                WHEN POSITION('/' IN scraped_date) > 0 THEN 
                    TO_TIMESTAMP(scraped_date, 'DD/MM/YYYY')
                ELSE 
                    TO_TIMESTAMP(scraped_date, 'YYYY-MM-DD')
            END
        ) AS scraped_date

    FROM {{ ref('b_listings') }}
    GROUP BY 
        CASE 
            WHEN listing_neighbourhood IS NULL 
                 OR TRIM(LOWER(listing_neighbourhood)) IN ('nan', 'none', 'overseas', '') 
            THEN 'no address'
            ELSE TRIM(LOWER(listing_neighbourhood))
        END
),

lga AS (
    SELECT 
        LOWER(TRIM(lga_name)) AS lga_name,
        CAST(lga_code AS VARCHAR) AS lga_code
    FROM {{ ref('b_lga_code') }}
)

SELECT
    ls.listing_neighbourhood,
    COALESCE(l.lga_code, 'unknown') AS lga_code,
    l.lga_name,
    ls.scraped_date
FROM listings ls
LEFT JOIN lga l 
    ON ls.listing_neighbourhood = l.lga_name
ORDER BY listing_neighbourhood
