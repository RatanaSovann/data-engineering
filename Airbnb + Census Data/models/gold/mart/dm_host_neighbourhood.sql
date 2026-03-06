{{
    config(
        alias='dm_host_neighbourhood'
    )
}}

WITH listing_data AS (
    SELECT
        fl.listing_id,
        fl.host_id,
        CONCAT(LPAD(EXTRACT(MONTH FROM fl.scraped_date)::text, 2, '0'), '-', EXTRACT(YEAR FROM fl.scraped_date)::text) AS month_year,
        fl.availability_30,
        fl.price,
        l.has_availability,
        hn.lga_name AS host_neighbourhood_lga
    FROM {{ ref('g_fact_listing') }} fl
    LEFT JOIN {{ ref('g_dim_listing') }} l 
        ON fl.listing_id = l.listing_id      
    LEFT JOIN {{ ref('g_dim_host_neighbourhood') }} hn 
        ON fl.host_neighbourhood = hn.host_neighbourhood 
),

deduplicated_listings AS (
    SELECT DISTINCT
        host_id,
        month_year,
        availability_30,
        price,
        has_availability,
        host_neighbourhood_lga
    FROM listing_data
),

joined_data AS (
    SELECT
        host_neighbourhood_lga,
        month_year,

        -- Number of Distinct Hosts
        COUNT(DISTINCT host_id) AS distinct_hosts,

        -- Estimated Revenue for Active Listings
        SUM(CASE WHEN has_availability THEN (30 - availability_30) * price ELSE 0 END) AS total_estimated_revenue,

        -- Estimated Revenue per Host
        ROUND(
            SUM(CASE WHEN has_availability THEN (30 - availability_30) * price ELSE 0 END)
            / NULLIF(COUNT(DISTINCT host_id), 0),
            2
        ) AS estimated_revenue_per_host
    FROM deduplicated_listings
    GROUP BY host_neighbourhood_lga, month_year
)

SELECT
    host_neighbourhood_lga,
    month_year,
    distinct_hosts,
    total_estimated_revenue,
    estimated_revenue_per_host
FROM joined_data
ORDER BY host_neighbourhood_lga, TO_DATE(month_year, 'MM-YYYY')
