{{
    config(
        alias='dm_listing_neighbourhood'
    )
}}

WITH listing_agg AS (
    SELECT
        listing_id,
        BOOL_OR(has_availability) AS has_availability,
        MAX(property_type) AS property_type,
        MAX(room_type) AS room_type
    FROM {{ ref('g_dim_listing') }}
    GROUP BY listing_id
),

review_agg AS (
    SELECT
        listing_id,
        AVG(review_scores_rating) AS avg_review_scores_rating
    FROM {{ ref('g_fact_reviews') }}
    GROUP BY listing_id
),

host_agg AS (
    SELECT
        host_id,
        BOOL_OR(is_superhost) AS is_superhost
    FROM {{ ref('g_dim_host') }}
    GROUP BY host_id
),

joined_data AS (
    SELECT
        fl.listing_id,
        fl.host_id,
        fl.listing_neighbourhood,
        TO_CHAR(fl.scraped_date, 'MM-YYYY') AS month_year,
        fl.accommodates,
        fl.availability_30,
        fl.price,
        la.has_availability,
        la.property_type,
        la.room_type,
        ra.avg_review_scores_rating,
        ha.is_superhost
    FROM {{ ref('g_fact_listing') }} fl
    LEFT JOIN listing_agg la ON fl.listing_id = la.listing_id
    LEFT JOIN review_agg ra ON fl.listing_id = ra.listing_id
    LEFT JOIN host_agg ha ON fl.host_id = ha.host_id
),

-- Step 1: Monthly aggregation
monthly_agg AS (
    SELECT
        listing_neighbourhood,
        month_year,
        SUM(CASE WHEN has_availability THEN 1 ELSE 0 END) AS total_active_listings,
        SUM(CASE WHEN NOT has_availability THEN 1 ELSE 0 END) AS total_inactive_listings,
        MIN(CASE WHEN has_availability THEN price END) AS min_price,
        MAX(CASE WHEN has_availability THEN price END) AS max_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN has_availability THEN price END) AS median_price,
        ROUND(AVG(CASE WHEN has_availability THEN price END), 2) AS avg_price,
        COUNT(DISTINCT host_id) AS distinct_hosts,
        ROUND(
            (COUNT(DISTINCT CASE WHEN is_superhost THEN host_id END) * 100.0 / NULLIF(COUNT(DISTINCT host_id), 0)),
        2) AS superhost_rate,
        ROUND(AVG(CASE WHEN has_availability THEN avg_review_scores_rating END), 2) AS avg_review_scores_rating,
        SUM(CASE WHEN has_availability THEN (30 - availability_30) ELSE 0 END) AS total_stays,
        ROUND(AVG(CASE WHEN has_availability THEN (30 - availability_30) * price ELSE NULL END), 2)
            AS avg_estimated_revenue_per_active_listing
    FROM joined_data
    GROUP BY listing_neighbourhood, month_year
),

-- Step 2: Month-over-month percentage changes
final AS (
    SELECT
        *,
        ROUND(
            (total_active_listings - LAG(total_active_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY TO_DATE(month_year, 'MM-YYYY')))
            * 100.0 / NULLIF(LAG(total_active_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY TO_DATE(month_year, 'MM-YYYY')), 0),
            2
        ) AS pct_change_active_listings,
        ROUND(
            (total_inactive_listings - LAG(total_inactive_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY TO_DATE(month_year, 'MM-YYYY')))
            * 100.0 / NULLIF(LAG(total_inactive_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY TO_DATE(month_year, 'MM-YYYY')), 0),
            2
        ) AS pct_change_inactive_listings
    FROM monthly_agg
)

SELECT *
FROM final
ORDER BY listing_neighbourhood, TO_DATE(month_year, 'MM-YYYY')
