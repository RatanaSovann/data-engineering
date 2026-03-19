USE DATABASE ASSIGNMENT_1;

SELECT *
FROM TABLE_YOUTUBE_FINAL;

/*
    Q1. What are the 3 most viewed videos for each country in the Gaming category for the trending_date = "2024-04-01"
*/ 

SELECT
    country,
    title,
    channel_title,
    view_count,
    ROW_NUMBER() OVER (      --Group by country, order each row by view count in DESC order and label each row 1,2,3...
        PARTITION BY country     
        ORDER BY view_count DESC
    ) AS rk
FROM table_youtube_final
WHERE category_title = 'Gaming'
  AND trending_date = '2024-04-01'
QUALIFY rk <= 3      -- Filter only 1,2,3 (top 3 view for each country)
ORDER BY country, rk; 

/*
    Q2. For each country, count the number of distinct video with a title containing the word “BTS” (case insensitive) and order the result by count in a             descending order
*/ 

SELECT 
    COUNTRY, 
    COUNT(DISTINCT VIDEO_ID) AS CT,
FROM table_youtube_final
WHERE TITLE ILIKE '%BTS%'
GROUP BY COUNTRY
ORDER BY CT DESC;

/*
    Q3. For each country, year and month (in a single column) and only for the year 2024, which video is the most viewed and what is its likes_ratio                  defined as the percentage of likes against view_count) truncated to 2 decimals. Order the result by year_month and country.
    
*/ 

WITH monthly_stats AS (
    SELECT 
        country,
        TO_CHAR(trending_date, 'YYYY-MM') AS year_month,
        title,
        channel_title AS CHANNELTITLE,
        CATEGORY_TITLE,
        view_count,
        TRUNC((likes::decimal / NULLIF(view_count,0)) * 100, 2) AS likes_ratio,
        ROW_NUMBER() OVER (
            PARTITION BY country, TO_CHAR(trending_date, 'YYYY-MM')
            ORDER BY view_count DESC
        ) AS rk
    FROM table_youtube_final
    WHERE EXTRACT(YEAR FROM trending_date) = 2024
)
SELECT country, year_month, title, view_count, likes_ratio
FROM monthly_stats
WHERE rk = 1
ORDER BY year_month, country;
    
/*
    Q4. For each country, which category_title has the most distinct videos and what is its percentage (2 decimals) out of the total distinct number of         videos of that country? Only look at the data from 2022. Order the result by category_title and country.
*/ 

-- Filtered to only from 2022
WITH filtered AS (
  SELECT country, category_title, video_id
  FROM table_youtube_final
  WHERE EXTRACT(YEAR FROM trending_date) >= 2022
),
-- Count number of distinct video for each category in each country
category_counts AS (
  SELECT
    country,
    category_title,
    COUNT(DISTINCT video_id) AS total_category_video
  FROM filtered
  GROUP BY country, category_title
),
-- Count number of distinct video for each country
country_totals AS (
  SELECT
    country,
    COUNT(DISTINCT video_id) AS total_country_video
  FROM filtered
  GROUP BY country
)
-- Join the two table together selecting relavent fields, using ROW_NUMBER to select only the top category_title with the highest total_category_video
SELECT
  cc.country,
  cc.category_title,
  cc.total_category_video,
  ct.total_country_video,
  ROUND(100.0 * cc.total_category_video / NULLIF(ct.total_country_video, 0), 2) AS percentage
FROM category_counts cc
JOIN country_totals ct ON ct.country = cc.country
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY cc.country
  ORDER BY cc.total_category_video DESC
) = 1
ORDER BY cc.category_title, cc.country;

/*
    Q5. Which channeltitle has produced the most distinct videos and what is this number? 
*/ 

SELECT
    CHANNEL_TITLE,
    COUNT(DISTINCT VIDEO_ID) AS n_unique
FROM table_youtube_final
GROUP BY CHANNEL_TITLE
ORDER BY n_unique DESC;

