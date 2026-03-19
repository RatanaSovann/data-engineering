#  Youtube Trending Insight

<img width="843" height="254" alt="image" src="https://github.com/user-attachments/assets/51146397-558a-4856-913c-fe816fbf1f96" />

## Objective

This project involve analysing a large dataset using a Data Lakehouse with Snowflakes. It involves:
1. Analyse YouTube trending data using Snowflake and Azure cloud storage
2. Clean and preprocess data (duplicates, missing values, inconsistent formats)
3. Perform SQL analysis to uncover trending patterns and answer research questions
4. Identify top-performing categories by engagement and speed to trend across countries
5. Generate actionable insights and recommendations for YouTube content strategy

## Dataset Used

A dataset with a daily record of the top trending YouTube videos has been extracted through the Youtube API and made available on the Kaggle (https://www.kaggle.com/rsrishav/youtube-trending-video-dataset)

The dataset used in this analysis consists of two types of files: CSV files containing trending video information and JSON files providing category titles for each country.
After uploading to Azure, the data was ingested into Snowflake and stored in internal tables. Data cleaning included handling null values and ensuring consistency. The CSV and JSON files were then merged into a consolidated table "table_youtube_final", with additional preprocessing such as creating unique identifiers and removing duplicates, forming a clean dataset ready for analysis.

## Technologies used:

- Language: SQL
- Extraction and transformation: Snowflake SQL
- Storage: Azure Blob Storage
- Data Warehouse: Snowflake Data Lakehouse
- Data Processing: Snowflake (internal tables, data cleaning, merging)

## Overview of Data Pre-processing:
<img width="1125" height="793" alt="image" src="https://github.com/user-attachments/assets/f8668528-e090-4821-b65d-73d556408caa" />

The final table contains the following 13 fields: Video ID, Title, Published Date, Channel ID, Channel Title, Category ID, Category Title, Trending Date, View Count, Likes, Dislikes, Comment Count, Country.

Full Snowflake SQL steps can be found here: 
[data_ingestion.sql](https://github.com/RatanaSovann/data-engineering/blob/main/Data%20Lakehouse/data_ingestion.sql) &
[data_cleaning.sql](https://github.com/RatanaSovann/data-engineering/blob/main/Data%20Lakehouse/data_cleaning.sql)

## Data Analysis:
To gain some familiarity and insights from the YouTube trending dataset, the analysis was structured around several targeted research questions:

**1. Top Performing Videos in Gaming: Identify the three most viewed videos in the Gaming category for each country on a specific trending date (April 1, 2024). This provides insight into regional differences in gaming content popularity.**

````sql
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
````

#### Output: 
<img width="925" height="291" alt="image" src="https://github.com/user-attachments/assets/f53b1a2d-f8f5-472a-b138-66a2ab414e5f" />


Across most countries, Clash Royale consistently emerges as the dominant gaming-related trending video, reflecting its global popularity within the YouTube gaming community.

***

**2. Prevalence of BTS-Related Content: For each country, count the number of unique videos with titles containing the word “BTS” (case-insensitive). The results are ranked to determine where BTS-related content is most dominant.**

````sql
/*
    Q2. For each country, count the number of distinct video with a title containing the word “BTS” (case insensitive) and order the result by count in a descending order
*/ 

SELECT 
    COUNTRY, 
    COUNT(DISTINCT VIDEO_ID) AS CT,
FROM table_youtube_final
WHERE TITLE ILIKE '%BTS%'
GROUP BY COUNTRY
ORDER BY CT DESC;
````

#### Output: 
<img width="193" height="320" alt="image" src="https://github.com/user-attachments/assets/825610d9-6d0a-4765-9234-300d54beb6ca" />

Cultural Influence of BTS: As expected, BTS-related videos trend most strongly in South Korea, followed by India (likely influenced by its large population and strong fan base) and the United States. This pattern highlights both regional fan engagement and the global reach of BTS content.

***

**3. Monthly Leaders and Engagement Ratios: For the year 2024, determine which video in each country achieves the highest views for every month. For these videos, calculate the likes ratio (likes as a percentage of total views), providing a measure of audience engagement beyond raw view counts.**

````sql
/*
    Q3. For each country, year and month (in a single column) and only for the year 2024, which video is the most viewed and what is its likes_ratio defined as the percentage of likes against view_count) truncated to 2 decimals. Order the result by year_month and country.
    
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
````
#### Output: 
<img width="603" height="349" alt="image" src="https://github.com/user-attachments/assets/e7816b55-22d8-4312-a85e-ee89d2a325fc" />

Monthly Leaders in 2024: The most-viewed video of the year appears in April 2024, with Discord Lootbox achieving a significantly higher view count than other months, a trend observed consistently across major regions. In addition, Grand Theft Auto videos recorded the highest like ratio in the U.S. in January 2024, suggesting particularly strong engagement from American audiences.

***

**4. Dominant Categories by Country (2022):For each country, identify the category with the largest number of distinct videos in 2022 and compute its share as a percentage of the country’s total distinct videos. This highlights category-level dominance within regional markets.**

````sql
/*
    Q4. For each country, which category_title has the most distinct videos and what is its percentage (2 decimals) out of the total distinct number of videos of that country? Only look at the data from 2022. Order the result by category_title and country.
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
````
#### Output: 
<img width="872" height="320" alt="image" src="https://github.com/user-attachments/assets/5c631455-25e7-4640-b2e5-9d58e8494578" />

Category Dominance in 2022: For most countries, the Entertainment category accounted for the largest share of distinct videos in 2022, underscoring its broad appeal. An exception is seen in Canada and the United States, where Gaming surpassed Entertainment in producing the most distinct trending videos.

***

**5. Most Prolific Channel Overall: Identify the channel that has produced the largest number of distinct videos across the entire dataset. This pinpoints the most consistently active content creator on YouTube.**

````sql
/*
    Q5. Which channeltitle has produced the most distinct videos and what is this number? 
*/ 

SELECT
    CHANNEL_TITLE,
    COUNT(DISTINCT VIDEO_ID) AS n_unique
FROM table_youtube_final
GROUP BY CHANNEL_TITLE
ORDER BY n_unique DESC;
````
#### Output: 
<img width="308" height="349" alt="image" src="https://github.com/user-attachments/assets/be71379e-6200-4d8f-a57d-c6e30b80af2d" />

Vijay Television was identified as the most prolific channel overall, producing 2,049 distinct videos, far exceeding other content creators in volume.

***
## Business Recommendation:

The business question driving this analysis is: If a new YouTube channel were launched tomorrow, which category of video—excluding “Music” and “Entertainment”—would be most likely to appear in the trending list?

To answer this, the first metric introduced is the lag time to trend. This is defined as the difference between a video’s published date and its trending date. A shorter lag suggests that videos in that category are able to capture audience attention more quickly, reflecting faster traction and a higher chance of virality.

<img width="1321" height="400" alt="image" src="https://github.com/user-attachments/assets/9ce7442f-bd8b-4a3a-a41d-103871e43ea4" />
<p align="center">
  Fig 2. Average days between published date and trending date by category

Science & Technology, Auto & Vehicles, and Sports categories has higher chance of going viral faster than other categories with Nonprofits & Activism being the slowest.

<img width="1314" height="306" alt="image" src="https://github.com/user-attachments/assets/990b0fc0-18c4-46ea-b9e4-a840a7416262" />
<p align="center">
  Fig 3. Average days between published date and trending date by country
  
  Trending videos published in India on average become viral a day faster than other countries, with Korea being the slowest (almost 7 days on average for a video to become trending).

***

The second metric is the engagement score, calculated as the average of likes and comments divided by the average lag in days. This measure balances the volume of engagement with the speed at which it is achieved. A higher engagement score indicates that a category generates stronger interaction in a shorter period, making it both viral and engaging.


<img width="1323" height="259" alt="image" src="https://github.com/user-attachments/assets/132965ae-32fc-47a1-b156-e98075680863" />
<p align="center">
  Fig 4. Sum of Engagement Score Across Countries Group by Category

Pets & Animals video receive the most positive engagement for viewers across all countries, followed by Comedy, Science & Technology.

<img width="1342" height="259" alt="image" src="https://github.com/user-attachments/assets/51288945-5b20-452f-abd1-93a52c351e7f" />
<p align="center">
  Fig 5.Top two categories for each country by average lag days and average engagement

A table with the top two categories with the highest average engagement score for each category grouped by country is also provided to help determine which category is most favourable by each region.

By combining lag time and engagement score, the analysis provides a systematic framework for identifying categories with the highest viral potential. These insights help to pinpoint where new creators should focus their efforts to maximize the likelihood of trending success on YouTube.

Based on the data above, it seems like posting Pets & Animal video will likely give you the most chance of going trending on YouTube, especially in India.

## Key Challenges
One of the main challenges I faced during this project was that SQL was a completely new language for me. At the beginning, I struggled with understanding query structure and translating business questions into SQL logic. Writing correct queries required multiple iterations and debugging, which proved to be the most time-consuming aspect of the analysis.
Through this process, however, I acquired several important SQL skills. I learned how to use commands such as ILIKE for case-insensitive string matching, ROW_NUMBER() and RANK() for ranking and filtering records, as well as advanced concepts like window functions. Although the steep learning curve presented difficulties, the project was ultimately a valuable learning experience.

