{{ 
    config(
        alias='s_dim_host',
        unique_key='host_id',
    )
}}

WITH new_data AS (
    SELECT DISTINCT
        CAST(host_id AS INT) AS host_id,

        CASE 
            WHEN host_name IS NULL OR LOWER(TRIM(host_name)) IN ('nan', 'none', '') 
            THEN CAST(host_id AS VARCHAR)
            ELSE LOWER(TRIM(host_name))
        END AS host_name,

        CASE 
            WHEN host_since IS NULL OR LOWER(TRIM(host_since)) IN ('nan', 'none', '') 
            THEN '1900-01-01'::DATE
            WHEN POSITION('/' IN host_since) > 0 THEN TO_DATE(host_since, 'DD/MM/YYYY')
            ELSE TO_DATE(host_since, 'YYYY-MM-DD')
        END AS host_since,

        CASE WHEN host_is_superhost = 't' THEN TRUE ELSE FALSE END AS is_superhost,

        CASE
            WHEN host_neighbourhood IS NULL OR TRIM(LOWER(host_neighbourhood)) IN ('nan', 'none', 'overseas', '')
            THEN 'no address'
            ELSE TRIM(LOWER(host_neighbourhood))
        END AS host_neighbourhood,

        CASE 
            WHEN scraped_date IS NULL OR LOWER(TRIM(scraped_date)) IN ('nan', 'none', '') THEN NOW()::timestamp
            WHEN POSITION('/' IN scraped_date) > 0 THEN TO_TIMESTAMP(scraped_date, 'DD/MM/YYYY')
            ELSE TO_TIMESTAMP(scraped_date, 'YYYY-MM-DD')
        END AS scraped_date

    FROM {{ ref('b_listings') }}
    WHERE host_id IS NOT NULL
      AND scraped_date >= (
        {% if is_incremental() %}
          -- Only process new data
          (SELECT COALESCE(MAX(scraped_date), '1900-01-01') FROM {{ this }})
        {% else %}
          '1900-01-01'
        {% endif %}
      )
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY host_id ORDER BY scraped_date DESC) AS rn
    FROM new_data
)

SELECT 
    host_id, host_name, host_since, is_superhost, host_neighbourhood, scraped_date
FROM deduped
WHERE rn = 1
