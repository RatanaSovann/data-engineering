{{
    config(
        unique_key='host_neighbourhood',
        alias='s_dim_host_neighbourhood'
    )
}}

WITH cleaned_listings AS (
    SELECT
        -- Standardize the host_neighbourhood field
        CASE 
            WHEN TRIM(LOWER(host_neighbourhood)) IN ('nan', 'overseas', '', 'none')
                 OR host_neighbourhood IS NULL 
            THEN 'no address'
            ELSE TRIM(LOWER(host_neighbourhood))
        END AS host_neighbourhood,
        
        -- Handle different date formats, replacing 'NaN' with current timestamp
        CASE 
            WHEN scraped_date IS NULL OR TRIM(LOWER(scraped_date)) = 'nan' THEN NOW()::timestamp
            WHEN POSITION('/' IN scraped_date) > 0 THEN 
                TO_TIMESTAMP(scraped_date, 'DD/MM/YYYY')
            ELSE 
                TO_TIMESTAMP(scraped_date, 'YYYY-MM-DD')
        END AS scraped_date
    FROM {{ ref('b_listings') }}
),

matched_neighbourhoods AS (
    SELECT DISTINCT
        cl.host_neighbourhood,
        COALESCE(
            TRIM(LOWER(ls.lga_name)),
            'no address'
        ) AS lga_name,
        MAX(cl.scraped_date) AS scraped_date
    FROM cleaned_listings cl
    LEFT JOIN {{ ref('b_lga_suburb') }} ls 
        ON TRIM(LOWER(cl.host_neighbourhood)) = TRIM(LOWER(ls.suburb_name))
    GROUP BY cl.host_neighbourhood, COALESCE(TRIM(LOWER(ls.lga_name)), 'no address')
),

lga_codes AS (
    SELECT DISTINCT
        mn.host_neighbourhood,
        COALESCE(lc.lga_code, 'unknown') AS lga_code,
        COALESCE(mn.lga_name, 'no address') AS lga_name,
        mn.scraped_date
    FROM matched_neighbourhoods mn
    LEFT JOIN {{ ref('b_lga_code') }} lc
        ON TRIM(LOWER(mn.lga_name)) = TRIM(LOWER(lc.lga_name))
)

SELECT 
    host_neighbourhood,
    lga_code,
    lga_name,
    scraped_date
FROM lga_codes
ORDER BY host_neighbourhood
