{{
    config(
        unique_key='listing_neighbourhood',
        alias='g_dim_listing_neighbourhood'
    )
}}

WITH source AS (
    SELECT * 
    FROM {{ ref('snapshot_dim_listing_neighbourhood') }}
),

cleaned AS (
    SELECT
        listing_neighbourhood,
        lga_name,
        lga_code,
        scraped_date,
        CASE 
            WHEN dbt_valid_from = (SELECT MIN(dbt_valid_from) FROM source) 
            THEN '1900-01-01'::timestamp 
            ELSE dbt_valid_from 
        END AS valid_from,
        dbt_valid_to AS valid_to
    FROM source
),

unknown AS (
    SELECT
        'unknown' AS listing_neighbourhood,
        'unknown' AS lga_name,
        '0' AS lga_code,                       
        '1900-01-01'::timestamp AS scraped_date,
        '1900-01-01'::timestamp AS valid_from,
        NULL::timestamp AS valid_to
)

SELECT * FROM unknown
UNION ALL
SELECT * FROM cleaned
