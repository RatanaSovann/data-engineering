{{
    config(
        unique_key='id',
        alias='listings'
    )
}}

WITH source AS (
    -- Select all data from the referenced b_listings table
    SELECT * FROM {{ ref('b_listings') }}
),
cleaned_data AS (
  SELECT
    id, 
    -- Cast the necessary columns to appropriate data types
    CAST(LISTING_ID AS INT) AS listing_id,
    CAST(SCRAPE_ID AS BIGINT) AS scrape_id,
    
    -- Handle different date formats, replacing 'NaN' with NULL
    CASE 
      WHEN SCRAPED_DATE = 'NaN' THEN NULL
      WHEN POSITION('/' IN SCRAPED_DATE) > 0 THEN 
        TO_DATE(SCRAPED_DATE, 'DD/MM/YYYY')
      ELSE 
        TO_DATE(SCRAPED_DATE, 'YYYY-MM-DD')
    END AS scraped_date, 

    CASE 
      WHEN HOST_SINCE = 'NaN' THEN NULL
      WHEN POSITION('/' IN HOST_SINCE) > 0 THEN 
        TO_DATE(HOST_SINCE, 'DD/MM/YYYY') 
      ELSE 
        TO_DATE(HOST_SINCE, 'YYYY-MM-DD')
    END AS host_since,

    CAST(HOST_ID AS INT) AS host_id,
    
    -- Convert host name to lowercase
    LOWER(HOST_NAME) AS host_name,
    
    -- Determine if the host is a superhost
    CASE 
      WHEN HOST_IS_SUPERHOST = 't' THEN TRUE
      ELSE FALSE 
    END AS is_superhost,
    
    -- Replace 'NaN' in host_neighbourhood with 'no address' and convert to lowercase
    LOWER(CASE 
      WHEN HOST_NEIGHBOURHOOD = 'NaN' THEN 'no address' 
      ELSE HOST_NEIGHBOURHOOD 
    END) AS host_neighbourhood,
    
    -- Replace 'NaN' in listing_neighbourhood with 'no address' and convert to lowercase
    LOWER(CASE 
      WHEN LISTING_NEIGHBOURHOOD = 'NaN' THEN 'no address' 
      ELSE LISTING_NEIGHBOURHOOD 
    END) AS listing_neighbourhood,
    
    -- Convert property_type and room_type to lowercase
    LOWER(PROPERTY_TYPE) AS property_type,
    LOWER(ROOM_TYPE) AS room_type,
    
    -- Cast accommodates to INT
    CAST(ACCOMMODATES AS INT) AS accommodates,
    
    -- Handle price: set to NULL if it's 0 or NULL, otherwise cast to DECIMAL
    CASE 
      WHEN PRICE IS NULL OR PRICE = '0' OR PRICE = 'NaN' THEN 0
      ELSE CAST(PRICE AS DECIMAL(10, 2))
    END AS price,
    
    -- Convert has_availability to boolean
    CASE 
        WHEN HAS_AVAILABILITY = 't' THEN TRUE
        ELSE FALSE
    END AS has_availability,
    
    -- Cast availability_30 to INT
    CASE 
      WHEN AVAILABILITY_30 IS NULL OR AVAILABILITY_30 = '0' OR AVAILABILITY_30 = 'NaN' THEN 0
      ELSE CAST(AVAILABILITY_30 AS INT)
    END AS availability_30,
    -- CAST(AVAILABILITY_30 AS INT) AS availability_30,
    
    -- Cast number of reviews to INT
    CASE 
      WHEN NUMBER_OF_REVIEWS IS NULL OR NUMBER_OF_REVIEWS = '0' OR NUMBER_OF_REVIEWS = 'NaN' THEN 0
      ELSE CAST(NUMBER_OF_REVIEWS AS INT)
    END AS review_count,
    -- CAST(NUMBER_OF_REVIEWS AS INT) AS review_count,
    
    -- Replace 'NaN' in REVIEW_SCORES_RATING with 100, cast to DECIMAL with increased precision
    COALESCE(
      CAST(NULLIF(REVIEW_SCORES_RATING, 'NaN') AS DECIMAL(5, 2)),  -- Increased precision
      100 -- Replace with your actual median value
    ) AS review_scores_rating,
    
    -- Replace 'NaN' in other review score columns with 10, cast to DECIMAL with increased precision
    COALESCE(
      CAST(NULLIF(REVIEW_SCORES_ACCURACY, 'NaN') AS DECIMAL(5, 2)),
      10
    ) AS review_scores_accuracy,
    
    COALESCE(
      CAST(NULLIF(REVIEW_SCORES_CLEANLINESS, 'NaN') AS DECIMAL(5, 2)),
      10
    ) AS review_scores_cleanliness,
    
    COALESCE(
      CAST(NULLIF(REVIEW_SCORES_CHECKIN, 'NaN') AS DECIMAL(5, 2)),
      10
    ) AS review_scores_checkin,
    
    COALESCE(
      CAST(NULLIF(REVIEW_SCORES_COMMUNICATION, 'NaN') AS DECIMAL(5, 2)),
      10
    ) AS review_scores_communication,
    
    COALESCE(
      CAST(NULLIF(REVIEW_SCORES_VALUE, 'NaN') AS DECIMAL(5, 2)),
      100
    ) AS review_scores_value
  FROM source
)
SELECT * 
FROM cleaned_data 
WHERE listing_id IS NOT NULL
  OR host_id IS NOT NULL