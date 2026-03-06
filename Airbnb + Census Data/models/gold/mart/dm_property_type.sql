{{ 
    config(
        alias='dm_property_type'
    ) 
}}

-- ===========================================
-- Pre-aggregate small or repetitive tables
-- ===========================================
WITH reviews_agg AS (
    SELECT 
        id,
        ROUND(AVG(review_scores_rating), 2) AS review_scores_rating
    FROM {{ ref('g_fact_reviews') }}
    GROUP BY id
),

hosts_agg AS (
    SELECT 
        host_id,
         BOOL_OR(is_superhost) AS is_superhost
    FROM {{ ref('g_dim_host') }}
    GROUP BY host_id
),

-- ===========================================
-- Prepare main listing data (core fact)
-- ===========================================
base_listing AS (
    SELECT
        f.id,
        f.listing_id,
        f.host_id,
        DATE_TRUNC('month', f.scraped_date)::date AS month_year,
        f.accommodates,
        f.price,
        f.availability_30,
        l.property_type,
        l.room_type,
        l.has_availability
    FROM {{ ref('g_fact_listing') }} f
    LEFT JOIN {{ ref('g_dim_listing') }} l 
        ON f.id = l.id
),

-- ===========================================
-- Join everything together
-- ===========================================
joined AS (
    SELECT
        b.listing_id,
        b.host_id,
        b.month_year,
        b.accommodates,
        b.price,
        b.availability_30,
        b.property_type,
        b.room_type,
        b.has_availability,
        h.is_superhost,
        r.review_scores_rating
    FROM base_listing b
    LEFT JOIN hosts_agg h USING (host_id)
    LEFT JOIN reviews_agg r USING (id)
),

-- ===========================================
-- Aggregate metrics at property / room / month level
-- ===========================================
aggregated AS (
    SELECT
        property_type,
        room_type,
        accommodates,
        TO_CHAR(month_year, 'MM-YYYY') AS month_year,

        COUNT(listing_id) AS total_listings,
        SUM(CASE WHEN has_availability THEN 1 ELSE 0 END) AS active_listings,

        -- Active Listing Rate
        ROUND(SUM(CASE WHEN has_availability THEN 1 ELSE 0 END) * 100.0 / COUNT(listing_id), 2) AS active_listing_rate,

        -- Price metrics for active listings
        MIN(CASE WHEN has_availability THEN price END) AS min_price,
        MAX(CASE WHEN has_availability THEN price END) AS max_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN has_availability THEN price END) AS median_price,
        ROUND(AVG(CASE WHEN has_availability THEN price END), 2) AS avg_price,

        -- Distinct host counts
        COUNT(DISTINCT host_id) AS distinct_hosts,

        -- Superhost rate
        ROUND(
            COUNT(DISTINCT CASE WHEN is_superhost THEN host_id END) * 100.0 
            / NULLIF(COUNT(DISTINCT host_id), 0), 2
        ) AS superhost_rate,

        -- Average review score (active listings)
        ROUND(AVG(CASE WHEN has_availability THEN review_scores_rating END), 2) AS avg_review_scores_rating,

        -- Total stays (active listings)
        SUM(CASE WHEN has_availability THEN 30 - availability_30 ELSE 0 END) AS total_stays,

        -- Estimated revenue per active listing
        ROUND(
            SUM(CASE WHEN has_availability THEN (30 - availability_30) * price ELSE 0 END)
            / NULLIF(SUM(CASE WHEN has_availability THEN 1 ELSE 0 END), 0), 2
        ) AS avg_estimated_revenue_per_active_listing
    FROM joined
    GROUP BY property_type, room_type, accommodates, month_year
),

-- ===========================================
-- Add percentage change metrics
-- ===========================================
final_view AS (
    SELECT
        property_type,
        room_type,
        accommodates,
        month_year,
        active_listing_rate,
        min_price,
        max_price,
        median_price,
        avg_price,
        distinct_hosts,
        superhost_rate,
        avg_review_scores_rating,
        total_stays,
        avg_estimated_revenue_per_active_listing,

        -- Month-over-month change in active listings
        ROUND(
            (active_listings - LAG(active_listings) OVER (
                PARTITION BY property_type, room_type, accommodates 
                ORDER BY TO_DATE(month_year, 'MM-YYYY')
            )) * 100.0
            / NULLIF(LAG(active_listings) OVER (
                PARTITION BY property_type, room_type, accommodates 
                ORDER BY TO_DATE(month_year, 'MM-YYYY')
            ), 0), 2
        ) AS pct_change_active_listings,

        -- Month-over-month change in inactive listings
        ROUND(
            ((total_listings - active_listings)
              - LAG(total_listings - active_listings) OVER (
                    PARTITION BY property_type, room_type, accommodates 
                    ORDER BY TO_DATE(month_year, 'MM-YYYY')
                )
            ) * 100.0
            / NULLIF(LAG(total_listings - active_listings) OVER (
                    PARTITION BY property_type, room_type, accommodates 
                    ORDER BY TO_DATE(month_year, 'MM-YYYY')
                ), 0), 2
        ) AS pct_change_inactive_listings
    FROM aggregated
)

-- ===========================================
-- Final Output
-- ===========================================
SELECT *
FROM final_view
ORDER BY property_type, room_type, accommodates, TO_DATE(month_year, 'MM-YYYY')
