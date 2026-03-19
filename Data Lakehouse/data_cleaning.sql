USE DATABASE assignment_1;

SELECT *
FROM table_youtube_category;

/*
    Q1. In “table_youtube_category” which category_title has duplicates if we don’t take into account 
    the categoryid (return only a single row)?
*/

SELECT 
    COUNT(DISTINCT CATEGORY_TITLE) AS unique_count
FROM table_youtube_category;
--There is 31 unique category

SELECT 
    COUNT(DISTINCT CATEGORY_TITLE || '-' ||  CATEGORYID) AS unique_row
FROM table_youtube_category;
--There is 32 combination of category

SELECT 
    DISTINCT CATEGORY_TITLE, CATEGORYID
FROM table_youtube_category
ORDER BY 
    CATEGORY_TITLE, 
    CATEGORYID;
-- Comedy has two different CATEGORYID value

/*
    Q2. In “table_youtube_category” which category_title only appears in one country?
*/

SELECT 
    CATEGORY_TITLE, COUNT(DISTINCT COUNTRY) AS num_countries
FROM table_youtube_category
GROUP BY CATEGORY_TITLE
HAVING COUNT(DISTINCT COUNTRY) = 1;

-- Nonprofit & Activism only appears in one country

/*
    Q3. In “table_youtube_final”, what is the categoryid of the missing category_title?
*/

SELECT DISTINCT(CATEGORYID) AS CATEGORYID
FROM table_youtube_final
WHERE category_title IS NULL
-- The missing category_title has categoryid of 29

/*
    Q4. Update the table_youtube_final to replace the NULL values in category_title with the answer from the previous question.
*/

UPDATE table_youtube_final f
SET category_title = c.category_title
FROM ASSIGNMENT_1.PUBLIC.table_youtube_category c
WHERE f.category_title IS NULL AND f.categoryid = c.categoryid;

  -- Verify Changes
SELECT COUNT(*) AS total_missing
FROM table_youtube_final
WHERE category_title IS NULL AND categoryid = 29;

/*
    Q5. In “table_youtube_final”, which video doesn’t havea channell title (return only the title)?
*/

SELECT title, channel_title
FROM table_youtube_final
WHERE channel_title IS NULL;
-- The video titled Kala Official Teaser|Tovino Thomas|Rohith V S|Juvis Production|Adventure Company doesn't have a channel name.

/*
    Q6. Delete from “table_youtube_final“, any record with video_id = “#NAME?”
*/

SELECT COUNT(*) AS total_name_to_remove
FROM table_youtube_final
WHERE video_id = '#NAME?';
-- 32081 row to be deleted

DELETE FROM table_youtube_final
WHERE video_id = '#NAME?';

/*
    Q7. Create a new table called “table_youtube_duplicates”  containing only the “bad” duplicates by using the row_number() function.
*/

CREATE OR REPLACE TABLE table_youtube_duplicates AS
SELECT *
FROM table_youtube_final
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY video_id, country, trending_date
    ORDER BY published_at DESC
) > 1;

/*
    Q8. Delete the duplicates in “table_youtube_final“ by using “table_youtube_duplicates”.
*/

--Preview row deleted: 37466

SELECT f.*
FROM table_youtube_final f
JOIN table_youtube_duplicates d
  ON f.id = d.id;

DELETE FROM table_youtube_final f
USING table_youtube_duplicates d
WHERE f.id = d.id;

/*
    Q9. Count the number of rows in “table_youtube_final“
*/

SELECT COUNT(*) AS total_rows
FROM table_youtube_final;


