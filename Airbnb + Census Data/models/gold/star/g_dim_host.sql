{{  
    config(
        alias='g_dim_host',
        unique_key='host_id'
    )
}}

WITH source AS (
    SELECT *
    FROM {{ ref('snapshot_host') }}
),

cleaned AS (
    SELECT
        host_id,
        host_name,

        -- Handle multiple date formats in host_since
        CASE 
            WHEN LOWER(TRIM(host_since::text)) IN ('', 'nan', 'na', 'none', 'null') THEN NULL
            WHEN POSITION('/' IN host_since::text) > 0 THEN TO_DATE(host_since::text, 'DD/MM/YYYY')
            ELSE TO_DATE(host_since::text, 'YYYY-MM-DD')
        END AS host_since,

        is_superhost,
        host_neighbourhood,

        -- Handle possible inconsistent formats in scraped_date too
        CASE 
            WHEN LOWER(TRIM(scraped_date::text)) IN ('', 'nan', 'na', 'none', 'null') THEN NULL
            WHEN POSITION('/' IN scraped_date::text) > 0 THEN TO_TIMESTAMP(scraped_date::text, 'DD/MM/YYYY')
            ELSE TO_TIMESTAMP(scraped_date::text, 'YYYY-MM-DD')
        END AS scraped_date,

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
        0 AS host_id,
        'unknown'::varchar AS host_name,
        '1900-01-01'::date AS host_since,
        TRUE AS is_superhost,
        'unknown'::varchar AS host_neighbourhood,
        '1900-01-01'::timestamp AS scraped_date,
        '1900-01-01'::timestamp AS valid_from,
        NULL::timestamp AS valid_to
)

SELECT * 
FROM cleaned
UNION ALL
SELECT * 
FROM unknown
