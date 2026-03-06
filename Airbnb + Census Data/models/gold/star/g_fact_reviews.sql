{{
    config(
        unique_key='id',
        alias='g_fact_reviews'
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
    review_count,
    review_scores_rating, 
    review_scores_accuracy, 
    review_scores_cleanliness, 
    review_scores_checkin, 
    review_scores_communication, 
    review_scores_value
FROM {{ ref('s_listings') }}